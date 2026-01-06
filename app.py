"""
Power Monitor - Complete Application
=====================================
VERSION: 2.4.0 (Jan 6, 2026 - Berkeley Lab header row offset fix)

Automated interconnection queue monitoring for all 7 US ISOs.

Sources:
- CAISO: gridstatus library
- NYISO: Direct Excel
- ISO-NE: HTML scraping  
- SPP: Direct CSV
- MISO: Free JSON API
- ERCOT: gridstatus.Ercot()
- PJM: Berkeley Lab data (automated)

Expected: 8,500+ projects
"""

APP_VERSION = "2.4.0"

import os
import sys
import json
import hashlib
import logging
import re
import time
import sqlite3
import threading
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from functools import wraps

import requests
import pandas as pd
from bs4 import BeautifulSoup
from flask import Flask, render_template, jsonify, request, redirect, url_for, Response
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import gridstatus
    GRIDSTATUS_AVAILABLE = True
except ImportError:
    GRIDSTATUS_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-key-change-in-production')

DB_PATH = os.environ.get('DATABASE_PATH', '/app/data/power_monitor.db')
DATA_DIR = os.environ.get('DATA_DIR', '/app/data')


# =============================================================================
# Database
# =============================================================================
class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.local = threading.local()
        self._init_db()
    
    def _get_conn(self):
        if not hasattr(self.local, 'conn') or self.local.conn is None:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.local.conn.row_factory = sqlite3.Row
        return self.local.conn
    
    def _init_db(self):
        conn = self._get_conn()
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT UNIQUE,
                project_name TEXT,
                capacity_mw REAL,
                county TEXT,
                state TEXT,
                customer TEXT,
                utility TEXT,
                status TEXT,
                fuel_type TEXT,
                source TEXT,
                source_url TEXT,
                project_type TEXT,
                hunter_score INTEGER DEFAULT 0,
                data_hash TEXT,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS monitor_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT,
                sources_checked INTEGER,
                projects_found INTEGER,
                projects_stored INTEGER,
                duration_seconds REAL,
                details TEXT
            );
            
            CREATE TABLE IF NOT EXISTS sync_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                projects_found INTEGER,
                projects_new INTEGER,
                status TEXT,
                error_message TEXT
            );
            
            CREATE TABLE IF NOT EXISTS alert_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE,
                min_capacity INTEGER DEFAULT 200,
                states TEXT,
                active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_projects_utility ON projects(utility);
            CREATE INDEX IF NOT EXISTS idx_projects_state ON projects(state);
            CREATE INDEX IF NOT EXISTS idx_projects_type ON projects(project_type);
            CREATE INDEX IF NOT EXISTS idx_projects_score ON projects(hunter_score);
        ''')
        conn.commit()
    
    def execute(self, query, params=()):
        conn = self._get_conn()
        cursor = conn.execute(query, params)
        conn.commit()
        return cursor
    
    def fetchall(self, query, params=()):
        return self._get_conn().execute(query, params).fetchall()
    
    def fetchone(self, query, params=()):
        return self._get_conn().execute(query, params).fetchone()


db = Database(DB_PATH)


# =============================================================================
# Power Monitor Class
# =============================================================================
class HybridPowerMonitor:
    """Complete power monitor with all 7 ISOs + Berkeley Lab backup"""
    
    def __init__(self, min_capacity_mw=100):
        self.min_capacity_mw = min_capacity_mw
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        self.berkeley_lab_cache = {}  # Cache by utility
    
    def extract_capacity(self, value):
        if pd.isna(value) or value is None or value == '':
            return None
        text = str(value).replace(',', '').strip()
        for suffix in ['MW', 'mw', 'Mw', 'MEGAWATT']:
            text = text.replace(suffix, '')
        text = text.strip()
        try:
            capacity = float(text)
            return capacity if capacity >= self.min_capacity_mw else None
        except ValueError:
            match = re.search(r'(\d+\.?\d*)', text)
            if match:
                try:
                    return float(match.group(1)) if float(match.group(1)) >= self.min_capacity_mw else None
                except:
                    pass
        return None
    
    def generate_hash(self, data):
        key = f"{data.get('project_name', '')}_{data.get('capacity_mw', 0)}_{data.get('state', '')}_{data.get('utility', '')}"
        return hashlib.md5(key.lower().encode()).hexdigest()
    
    def classify_project(self, name, customer='', fuel_type=''):
        text = f"{name} {customer} {fuel_type}".lower()
        if any(kw in text for kw in ['data center', 'datacenter', 'cloud', 'hyperscale', 'colocation', 'microsoft', 'amazon', 'google', 'meta', 'aws', 'facebook']):
            return 'datacenter'
        if any(kw in text for kw in ['battery', 'storage', 'bess', 'energy storage']):
            return 'storage'
        if any(kw in text for kw in ['solar', 'photovoltaic', 'pv ']):
            return 'solar'
        if any(kw in text for kw in ['wind', 'offshore']):
            return 'wind'
        if any(kw in text for kw in ['natural gas', 'gas turbine', 'combined cycle', 'peaker', 'ccgt']):
            return 'gas'
        if any(kw in text for kw in ['nuclear']):
            return 'nuclear'
        return 'other'
    
    def calculate_hunter_score(self, project):
        """Calculate datacenter likelihood score (0-100)"""
        score = 0
        name = (project.get('project_name', '') + ' ' + project.get('customer', '')).lower()
        
        # Direct datacenter indicators (+40)
        dc_keywords = ['data center', 'datacenter', 'hyperscale', 'colocation', 'colo ', 'server farm']
        if any(kw in name for kw in dc_keywords):
            score += 40
        
        # Tech company names (+35)
        tech_companies = ['microsoft', 'amazon', 'aws', 'google', 'meta', 'facebook', 'apple', 'oracle', 'ibm', 
                         'digital realty', 'equinix', 'cyrusone', 'qts', 'coresite', 'vantage', 'cloudflare']
        if any(co in name for co in tech_companies):
            score += 35
        
        # Datacenter hotspot locations (+15)
        county = project.get('county', '').lower()
        state = project.get('state', '').upper()
        hotspots = [
            ('loudoun', 'VA'), ('prince william', 'VA'), ('fairfax', 'VA'),
            ('santa clara', 'CA'), ('maricopa', 'AZ'), ('douglas', 'GA'),
            ('dallas', 'TX'), ('fort worth', 'TX'),
        ]
        if any(h[0] in county and h[1] == state for h in hotspots):
            score += 15
        
        # High capacity bonus (+10)
        capacity = project.get('capacity_mw', 0)
        if capacity >= 500:
            score += 10
        elif capacity >= 200:
            score += 5
        
        # Load-only indicators (+20)
        if any(kw in name for kw in ['load', 'behind meter', 'btm', 'campus']):
            score += 20
        
        return min(score, 100)

    # =========================================================================
    # CAISO
    # =========================================================================
    def fetch_caiso(self):
        projects = []
        if not GRIDSTATUS_AVAILABLE:
            logger.warning("CAISO: gridstatus not available")
            return projects
        try:
            logger.info("CAISO: Fetching via gridstatus")
            caiso = gridstatus.CAISO()
            df = caiso.get_interconnection_queue()
            logger.info(f"CAISO: Found {len(df)} rows")
            
            for _, row in df.iterrows():
                capacity = self.extract_capacity(row.get('Capacity (MW)', 0))
                if capacity:
                    data = {
                        'request_id': f"CAISO_{row.get('Queue ID', row.name)}",
                        'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                        'capacity_mw': capacity,
                        'county': str(row.get('County', ''))[:200],
                        'state': 'CA',
                        'customer': str(row.get('Interconnection Customer', ''))[:500],
                        'utility': 'CAISO',
                        'status': str(row.get('Status', 'Active')),
                        'fuel_type': str(row.get('Fuel', '')),
                        'source': 'CAISO',
                        'source_url': 'gridstatus',
                        'project_type': self.classify_project(str(row.get('Project Name', '')), str(row.get('Interconnection Customer', '')), str(row.get('Fuel', '')))
                    }
                    data['hunter_score'] = self.calculate_hunter_score(data)
                    data['data_hash'] = self.generate_hash(data)
                    projects.append(data)
            logger.info(f"CAISO: Extracted {len(projects)} projects")
        except Exception as e:
            logger.error(f"CAISO failed: {e}")
        return projects

    # =========================================================================
    # NYISO
    # =========================================================================
    def fetch_nyiso(self):
        projects = []
        url = 'https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx'
        try:
            logger.info(f"NYISO: Fetching from {url}")
            response = self.session.get(url, timeout=60)
            if response.status_code == 200:
                df = pd.read_excel(BytesIO(response.content))
                logger.info(f"NYISO: Found {len(df)} rows")
                mw_cols = [c for c in df.columns if 'MW' in str(c).upper()]
                
                for _, row in df.iterrows():
                    capacity = None
                    for col in mw_cols:
                        capacity = self.extract_capacity(row.get(col))
                        if capacity:
                            break
                    if capacity:
                        data = {
                            'request_id': f"NYISO_{row.get('Queue Position', row.name)}",
                            'project_name': str(row.get('Project Name', row.get('Proposed Name', 'Unknown')))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': 'NY',
                            'customer': str(row.get('Developer', ''))[:500],
                            'utility': 'NYISO',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Type', '')),
                            'source': 'NYISO',
                            'source_url': url,
                            'project_type': self.classify_project(str(row.get('Project Name', '')), '', str(row.get('Type', '')))
                        }
                        data['hunter_score'] = self.calculate_hunter_score(data)
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                logger.info(f"NYISO: Extracted {len(projects)} projects")
        except Exception as e:
            logger.error(f"NYISO failed: {e}")
        return projects

    # =========================================================================
    # ISO-NE
    # =========================================================================
    def fetch_isone(self):
        projects = []
        url = 'https://irtt.iso-ne.com/reports/external'
        try:
            logger.info(f"ISO-NE: Fetching from {url}")
            response = self.session.get(url, timeout=60)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                table = soup.find('table')
                if table:
                    headers = [th.get_text(strip=True) for th in table.find_all('th')]
                    rows = table.find_all('tr')[1:]
                    logger.info(f"ISO-NE: Found {len(rows)} rows")
                    
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= len(headers):
                            row_data = {headers[i]: cells[i].get_text(strip=True) for i in range(len(headers))}
                            capacity = None
                            for mw_col in ['Net MW', 'Summer MW', 'Winter MW', 'MW']:
                                if mw_col in row_data:
                                    capacity = self.extract_capacity(row_data.get(mw_col))
                                    if capacity:
                                        break
                            if capacity:
                                data = {
                                    'request_id': f"ISONE_{row_data.get('QP', len(projects))}",
                                    'project_name': str(row_data.get('Alternative Name', row_data.get('Unit', 'Unknown')))[:500],
                                    'capacity_mw': capacity,
                                    'county': str(row_data.get('County', ''))[:200],
                                    'state': str(row_data.get('ST', 'MA'))[:2],
                                    'customer': '',
                                    'utility': 'ISO-NE',
                                    'status': str(row_data.get('Status', 'Active')),
                                    'fuel_type': str(row_data.get('Fuel Type', '')),
                                    'source': 'ISO-NE',
                                    'source_url': url,
                                    'project_type': self.classify_project(row_data.get('Alternative Name', ''), '', row_data.get('Fuel Type', ''))
                                }
                                data['hunter_score'] = self.calculate_hunter_score(data)
                                data['data_hash'] = self.generate_hash(data)
                                projects.append(data)
                    logger.info(f"ISO-NE: Extracted {len(projects)} projects")
        except Exception as e:
            logger.error(f"ISO-NE failed: {e}")
        return projects

    # =========================================================================
    # SPP
    # =========================================================================
    def fetch_spp(self):
        projects = []
        url = 'https://opsportal.spp.org/Studies/GenerateActiveCSV'
        try:
            logger.info(f"SPP: Fetching from {url}")
            response = self.session.get(url, timeout=60)
            if response.status_code == 200:
                lines = response.text.split('\n')
                header_idx = 0
                for i, line in enumerate(lines[:10]):
                    if 'MW' in line or 'Generation' in line:
                        header_idx = i
                        break
                csv_data = '\n'.join(lines[header_idx:])
                df = pd.read_csv(StringIO(csv_data))
                logger.info(f"SPP: Found {len(df)} rows")
                mw_cols = [c for c in df.columns if 'MW' in str(c).upper()]
                
                for _, row in df.iterrows():
                    capacity = None
                    for col in mw_cols:
                        capacity = self.extract_capacity(row.get(col))
                        if capacity:
                            break
                    if capacity:
                        data = {
                            'request_id': f"SPP_{row.get('Generation Interconnection Number', row.name)}",
                            'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get(' Nearest Town or County', ''))[:200],
                            'state': str(row.get('State', ''))[:2],
                            'customer': '',
                            'utility': 'SPP',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Fuel Type', row.get('Generation Type', ''))),
                            'source': 'SPP',
                            'source_url': url,
                            'project_type': self.classify_project(str(row.get('Project Name', '')), '', str(row.get('Fuel Type', '')))
                        }
                        data['hunter_score'] = self.calculate_hunter_score(data)
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                logger.info(f"SPP: Extracted {len(projects)} projects")
        except Exception as e:
            logger.error(f"SPP failed: {e}")
        return projects

    # =========================================================================
    # MISO - FREE JSON API (with gridstatus fallback)
    # =========================================================================
    def fetch_miso(self):
        projects = []
        
        # Try direct API first
        projects = self._fetch_miso_direct()
        
        # If direct API failed, try gridstatus
        if not projects and GRIDSTATUS_AVAILABLE:
            logger.info("MISO: Direct API returned no data, trying gridstatus fallback")
            projects = self._fetch_miso_gridstatus()
        
        return projects
    
    def _fetch_miso_direct(self):
        """Fetch MISO data directly from JSON API"""
        projects = []
        url = "https://www.misoenergy.org/api/giqueue/getprojects"
        try:
            logger.info(f"MISO: Fetching from JSON API (v{APP_VERSION})")
            response = self.session.get(url, timeout=60)
            
            if response.status_code != 200:
                logger.error(f"MISO: API returned status {response.status_code}")
                logger.error(f"MISO: Response text: {response.text[:500]}")
                return projects
            
            data = response.json()
            
            if not data:
                logger.warning("MISO: API returned empty data")
                return projects
            
            logger.info(f"MISO: Found {len(data)} rows from API")
            
            # Log sample record to debug field names
            if data and len(data) > 0:
                sample = data[0]
                logger.info(f"MISO: Sample record keys: {list(sample.keys())}")
                # Log a few key fields to see what we're getting
                sample_fields = {k: sample.get(k) for k in ['summerNetMW', 'winterNetMW', 'mw', 'MW', 'capacity', 'jNumber', 'projectName'] if k in sample}
                logger.info(f"MISO: Sample field values: {sample_fields}")
            
            for item in data:
                # Try multiple capacity fields
                capacity = None
                for cap_field in ['summerNetMW', 'winterNetMW', 'mw', 'MW', 'capacity', 'netMW', 'Capacity']:
                    cap_val = item.get(cap_field)
                    if cap_val is not None:
                        capacity = self.extract_capacity(cap_val)
                        if capacity:
                            break
                
                if capacity:
                    proj = {
                        'request_id': f"MISO_{item.get('jNumber', item.get('queueNumber', item.get('Queue Number', 'UNK')))}",
                        'project_name': str(item.get('projectName', item.get('name', item.get('Project Name', 'Unknown'))))[:500],
                        'capacity_mw': capacity,
                        'county': str(item.get('county', item.get('County', '')))[:200],
                        'state': str(item.get('state', item.get('State', '')))[:2],
                        'customer': str(item.get('interconnectionEntity', item.get('developer', item.get('Developer', ''))))[:500],
                        'utility': 'MISO',
                        'status': str(item.get('status', item.get('queueStatus', item.get('Status', 'Active')))),
                        'fuel_type': str(item.get('fuelType', item.get('fuel', item.get('Fuel Type', '')))),
                        'source': 'MISO',
                        'source_url': url,
                        'project_type': self.classify_project(
                            item.get('projectName', item.get('Project Name', '')), 
                            item.get('interconnectionEntity', item.get('Developer', '')), 
                            item.get('fuelType', item.get('Fuel Type', ''))
                        )
                    }
                    proj['hunter_score'] = self.calculate_hunter_score(proj)
                    proj['data_hash'] = self.generate_hash(proj)
                    projects.append(proj)
            
            logger.info(f"MISO: Extracted {len(projects)} projects (>= {self.min_capacity_mw} MW)")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"MISO: Network error: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"MISO: JSON parse error: {e}")
            logger.error(f"MISO: Response content: {response.text[:500]}")
        except Exception as e:
            logger.error(f"MISO: Unexpected error: {e}")
            import traceback
            logger.error(f"MISO: Traceback: {traceback.format_exc()}")
        
        return projects
    
    def _fetch_miso_gridstatus(self):
        """Fallback: Fetch MISO data via gridstatus library"""
        projects = []
        try:
            logger.info("MISO: Fetching via gridstatus.MISO()")
            miso = gridstatus.MISO()
            df = miso.get_interconnection_queue()
            logger.info(f"MISO: gridstatus returned {len(df)} rows")
            logger.info(f"MISO: gridstatus columns: {list(df.columns)[:10]}")
            
            for _, row in df.iterrows():
                # gridstatus normalizes the column name to 'Capacity (MW)'
                capacity = self.extract_capacity(row.get('Capacity (MW)') or row.get('summerNetMW') or row.get('winterNetMW') or 0)
                if capacity:
                    proj = {
                        'request_id': f"MISO_{row.get('Queue ID', row.get('jNumber', row.name))}",
                        'project_name': str(row.get('Project Name', row.get('projectName', 'Unknown')))[:500],
                        'capacity_mw': capacity,
                        'county': str(row.get('County', row.get('county', '')))[:200],
                        'state': str(row.get('State', row.get('state', '')))[:2],
                        'customer': str(row.get('Interconnecting Entity', row.get('interconnectionEntity', '')))[:500],
                        'utility': 'MISO',
                        'status': str(row.get('Status', row.get('status', 'Active'))),
                        'fuel_type': str(row.get('Fuel Type', row.get('fuelType', ''))),
                        'source': 'MISO',
                        'source_url': 'gridstatus',
                        'project_type': self.classify_project(
                            row.get('Project Name', row.get('projectName', '')),
                            row.get('Interconnecting Entity', row.get('interconnectionEntity', '')),
                            row.get('Fuel Type', row.get('fuelType', ''))
                        )
                    }
                    proj['hunter_score'] = self.calculate_hunter_score(proj)
                    proj['data_hash'] = self.generate_hash(proj)
                    projects.append(proj)
            
            logger.info(f"MISO: gridstatus extracted {len(projects)} projects")
            
        except Exception as e:
            logger.error(f"MISO: gridstatus fallback failed: {e}")
            import traceback
            logger.error(f"MISO: Traceback: {traceback.format_exc()}")
        
        return projects

    # =========================================================================
    # ERCOT - gridstatus.Ercot()
    # =========================================================================
    def fetch_ercot(self):
        projects = []
        if not GRIDSTATUS_AVAILABLE:
            logger.warning("ERCOT: gridstatus not available")
            return projects
        try:
            logger.info("ERCOT: Fetching via gridstatus.Ercot()")
            ercot = gridstatus.Ercot()  # Note: lowercase 'e'!
            df = ercot.get_interconnection_queue()
            logger.info(f"ERCOT: Found {len(df)} rows")
            
            for _, row in df.iterrows():
                capacity = self.extract_capacity(row.get('Capacity (MW)') or row.get('Summer MW') or 0)
                if capacity:
                    data = {
                        'request_id': f"ERCOT_{row.get('Queue ID', row.name)}",
                        'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                        'capacity_mw': capacity,
                        'county': str(row.get('County', ''))[:200],
                        'state': 'TX',
                        'customer': str(row.get('Interconnecting Entity', ''))[:500],
                        'utility': 'ERCOT',
                        'status': str(row.get('Status', 'Active')),
                        'fuel_type': str(row.get('Fuel', row.get('Technology', ''))),
                        'source': 'ERCOT',
                        'source_url': 'gridstatus',
                        'project_type': self.classify_project(str(row.get('Project Name', '')), str(row.get('Interconnecting Entity', '')), str(row.get('Fuel', '')))
                    }
                    data['hunter_score'] = self.calculate_hunter_score(data)
                    data['data_hash'] = self.generate_hash(data)
                    projects.append(data)
            logger.info(f"ERCOT: Extracted {len(projects)} projects")
        except Exception as e:
            logger.error(f"ERCOT failed: {e}")
        return projects

    # =========================================================================
    # PJM - Via Berkeley Lab (PJM API requires paid membership)
    # =========================================================================
    def fetch_pjm(self):
        """
        PJM data via Berkeley Lab's comprehensive dataset.
        Note: PJM's own API requires paid membership, so we use Berkeley Lab.
        """
        projects = []
        
        # Check cache first
        if 'PJM' in self.berkeley_lab_cache and self.berkeley_lab_cache['PJM']:
            logger.info(f"PJM: Using cached data ({len(self.berkeley_lab_cache['PJM'])} projects)")
            return self.berkeley_lab_cache['PJM']
        
        # Fetch from Berkeley Lab
        logger.info("PJM: Fetching from Berkeley Lab dataset")
        berkeley_projects = self.fetch_berkeley_lab()
        
        # Extract PJM projects
        pjm_projects = [p for p in berkeley_projects if p.get('utility') == 'PJM']
        
        if pjm_projects:
            self.berkeley_lab_cache['PJM'] = pjm_projects
            logger.info(f"PJM: Extracted {len(pjm_projects)} projects from Berkeley Lab")
        else:
            logger.warning("PJM: No data found in Berkeley Lab dataset")
        
        return pjm_projects

    # =========================================================================
    # Berkeley Lab - Comprehensive Data (with aggressive retry)
    # =========================================================================
    def fetch_berkeley_lab(self):
        """Fetch Berkeley Lab data with multiple strategies"""
        projects = []
        
        # Strategy 1: Try known URL patterns
        # WORKING URL - Note the specific capitalization is critical!
        urls_to_try = [
            # 2025 Edition - VERIFIED WORKING (user-provided, Jan 2026)
            'https://emp.lbl.gov/sites/default/files/2025-08/LBNL_Ix_Queue_Data_File_thru2024_v2.xlsx',
            # Alternative patterns with correct capitalization
            'https://emp.lbl.gov/sites/default/files/2025-12/LBNL_Ix_Queue_Data_File_thru2024_v2.xlsx',
            'https://eta-publications.lbl.gov/sites/default/files/2025-08/LBNL_Ix_Queue_Data_File_thru2024_v2.xlsx',
            # 2024 fallback
            'https://emp.lbl.gov/sites/default/files/2024-04/queued_up_2024_data_file.xlsx',
        ]
        
        # Different header combinations to try
        header_sets = [
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://emp.lbl.gov/queues',
                'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Origin': 'https://emp.lbl.gov',
            },
            {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
                'Referer': 'https://emp.lbl.gov/queues',
                'Accept': '*/*',
            },
            {
                'User-Agent': 'curl/7.88.1',
                'Accept': '*/*',
            },
        ]
        
        df = None
        successful_url = None
        excel_content = None  # Save for re-reading with correct header
        selected_sheet = None  # Save sheet name for re-reading
        
        for url in urls_to_try:
            for headers in header_sets:
                try:
                    logger.info(f"Berkeley Lab: Trying {url}")
                    response = self.session.get(url, headers=headers, timeout=120, allow_redirects=True)
                    
                    # Check if we got actual Excel data
                    if response.status_code == 200:
                        content_type = response.headers.get('Content-Type', '')
                        content_length = len(response.content)
                        
                        # Excel files should be > 100KB and have right content type or magic bytes
                        if content_length > 100000:
                            # Check for Excel magic bytes (PK for xlsx)
                            if response.content[:2] == b'PK' or 'spreadsheet' in content_type or 'excel' in content_type:
                                logger.info(f"Berkeley Lab: Downloaded {content_length/1024/1024:.1f} MB from {url}")
                                
                                # Try to find the correct sheet with project data
                                excel_file = pd.ExcelFile(BytesIO(response.content))
                                logger.info(f"Berkeley Lab: Found {len(excel_file.sheet_names)} sheets: {excel_file.sheet_names}")
                                
                                # Look for the data sheet by name first - be specific!
                                data_sheet = None
                                
                                # Priority 1: Look for "Complete" data sheets
                                for sheet_name in excel_file.sheet_names:
                                    sheet_lower = sheet_name.lower()
                                    if 'complete' in sheet_lower and ('data' in sheet_lower or 'queue' in sheet_lower):
                                        data_sheet = sheet_name
                                        logger.info(f"Berkeley Lab: Found complete data sheet: '{sheet_name}'")
                                        break
                                
                                # Priority 2: Look for "Full" data sheets
                                if data_sheet is None:
                                    for sheet_name in excel_file.sheet_names:
                                        sheet_lower = sheet_name.lower()
                                        if 'full' in sheet_lower and 'data' in sheet_lower:
                                            data_sheet = sheet_name
                                            logger.info(f"Berkeley Lab: Found full data sheet: '{sheet_name}'")
                                            break
                                
                                # Priority 3: Look for "All" requests/projects
                                if data_sheet is None:
                                    for sheet_name in excel_file.sheet_names:
                                        sheet_lower = sheet_name.lower()
                                        if 'all' in sheet_lower and ('request' in sheet_lower or 'project' in sheet_lower or 'queue' in sheet_lower):
                                            data_sheet = sheet_name
                                            logger.info(f"Berkeley Lab: Found all requests sheet: '{sheet_name}'")
                                            break
                                
                                # Priority 4: Generic data/queue but NOT sample/summary
                                if data_sheet is None:
                                    for sheet_name in excel_file.sheet_names:
                                        sheet_lower = sheet_name.lower()
                                        # Skip sample, summary, background, methods sheets
                                        if any(skip in sheet_lower for skip in ['sample', 'summary', 'background', 'method', 'intro', 'content', 'codebook']):
                                            continue
                                        if any(kw in sheet_lower for kw in ['data', 'queue', 'project', 'active', 'request']):
                                            data_sheet = sheet_name
                                            logger.info(f"Berkeley Lab: Found data sheet by name: '{sheet_name}'")
                                            break
                                
                                # If no obvious data sheet name, check each sheet for real data
                                if data_sheet is None:
                                    logger.info("Berkeley Lab: No obvious data sheet name, checking content...")
                                    best_sheet = None
                                    best_rows = 0
                                    
                                    for sheet_name in excel_file.sheet_names:
                                        try:
                                            # Read first few rows to check columns
                                            temp_df = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=10)
                                            cols_lower = [str(c).lower() for c in temp_df.columns]
                                            
                                            # Check for typical interconnection queue column names
                                            has_data_cols = any(
                                                kw in ' '.join(cols_lower) 
                                                for kw in ['entity', 'region', 'iso', 'queue', 'capacity', 'mw', 'state', 'county', 'developer']
                                            )
                                            
                                            if has_data_cols:
                                                full_df = pd.read_excel(excel_file, sheet_name=sheet_name)
                                                logger.info(f"Berkeley Lab: Sheet '{sheet_name}' has {len(full_df)} rows, data columns detected")
                                                if len(full_df) > best_rows:
                                                    best_rows = len(full_df)
                                                    best_sheet = sheet_name
                                            else:
                                                # Still count rows as fallback
                                                full_df = pd.read_excel(excel_file, sheet_name=sheet_name)
                                                if len(full_df) > 100 and len(full_df) > best_rows:
                                                    # Only use as fallback if no better option
                                                    if best_sheet is None:
                                                        best_rows = len(full_df)
                                                        best_sheet = sheet_name
                                        except Exception as e:
                                            logger.debug(f"Berkeley Lab: Error reading sheet '{sheet_name}': {e}")
                                            continue
                                    
                                    data_sheet = best_sheet
                                    if data_sheet:
                                        logger.info(f"Berkeley Lab: Selected sheet '{data_sheet}' with {best_rows} rows")
                                
                                # Read the data sheet
                                if data_sheet:
                                    df = pd.read_excel(excel_file, sheet_name=data_sheet)
                                    logger.info(f"Berkeley Lab: Loaded sheet '{data_sheet}' with {len(df)} rows")
                                else:
                                    # Last resort - try sheet index 1 (often data is on second sheet after cover)
                                    if len(excel_file.sheet_names) > 1:
                                        data_sheet = excel_file.sheet_names[1]
                                        df = pd.read_excel(excel_file, sheet_name=data_sheet)
                                        logger.info(f"Berkeley Lab: Using sheet index 1 ('{data_sheet}') with {len(df)} rows")
                                    else:
                                        data_sheet = excel_file.sheet_names[0]
                                        df = pd.read_excel(excel_file, sheet_name=data_sheet)
                                        logger.info(f"Berkeley Lab: Using sheet index 0 ('{data_sheet}') with {len(df)} rows")
                                
                                successful_url = url
                                excel_content = response.content  # Save for re-reading
                                selected_sheet = data_sheet  # Save sheet name
                                logger.info(f"Berkeley Lab: SUCCESS! Final sheet has {len(df)} rows")
                                break
                            else:
                                logger.debug(f"Berkeley Lab: Got response but not Excel (type: {content_type}, size: {content_length})")
                        else:
                            logger.debug(f"Berkeley Lab: Response too small ({content_length} bytes)")
                    else:
                        logger.debug(f"Berkeley Lab: HTTP {response.status_code} for {url}")
                        
                except Exception as e:
                    logger.debug(f"Berkeley Lab: Failed {url}: {e}")
                    continue
            
            if df is not None:
                break
        
        # Strategy 2: Check for locally cached file
        if df is None:
            local_paths = [
                os.path.join(DATA_DIR, 'queued_up_data.xlsx'),
                os.path.join(DATA_DIR, 'queued_up_2025_data_file.xlsx'),
                os.path.join(DATA_DIR, 'berkeley_lab.xlsx'),
                '/tmp/berkeley_lab_cache.xlsx',
            ]
            for path in local_paths:
                if os.path.exists(path):
                    try:
                        df = pd.read_excel(path, sheet_name=0)
                        successful_url = f"file://{path}"
                        logger.info(f"Berkeley Lab: Loaded from local cache: {path}")
                        break
                    except Exception as e:
                        logger.debug(f"Berkeley Lab: Failed to load {path}: {e}")
        
        if df is None:
            logger.error("Berkeley Lab: All fetch attempts failed")
            logger.info("Berkeley Lab: Download manually from https://emp.lbl.gov/queues and place in /app/data/")
            return projects
        
        # The sheet often has navigation/title rows at the top - find the actual header row
        # Look for a row that contains typical column names
        header_keywords = ['entity', 'region', 'queue', 'capacity', 'mw', 'state', 'county', 'status', 'resource', 'developer']
        header_row_idx = None
        
        # Check first 20 rows to find the header
        for idx in range(min(20, len(df))):
            row_values = [str(v).lower() for v in df.iloc[idx].values if pd.notna(v)]
            row_text = ' '.join(row_values)
            matches = sum(1 for kw in header_keywords if kw in row_text)
            if matches >= 3:  # Found at least 3 header keywords
                header_row_idx = idx
                logger.info(f"Berkeley Lab: Found header row at index {idx}: {row_values[:5]}")
                break
        
        if header_row_idx is not None:
            # The header_row_idx is relative to the DATA rows (after current header)
            # Since original read used header=0, the Excel row is header_row_idx + 1
            actual_header_row = header_row_idx + 1
            logger.info(f"Berkeley Lab: Actual Excel header row is {actual_header_row}")
            
            # Re-read with correct header row
            if excel_content is not None:
                # Re-read from the saved content
                excel_file = pd.ExcelFile(BytesIO(excel_content))
                df = pd.read_excel(excel_file, sheet_name=selected_sheet, header=actual_header_row)
            elif successful_url and not successful_url.startswith('http'):
                # Local file
                df = pd.read_excel(successful_url.replace('file://', ''), header=actual_header_row)
            logger.info(f"Berkeley Lab: Re-read with header at Excel row {actual_header_row}, now {len(df)} rows")
        
        # Clean column names (remove whitespace, normalize)
        df.columns = [str(c).strip() for c in df.columns]
        
        # Process the dataframe
        logger.info(f"Berkeley Lab: Processing {len(df)} rows")
        logger.info(f"Berkeley Lab: Columns: {list(df.columns)[:10]}")
        
        # Find columns
        def find_col(names):
            for name in names:
                for col in df.columns:
                    if name.lower() in str(col).lower():
                        return col
            return None
        
        entity_col = find_col(['entity', 'region', 'iso', 'rto', 'ba'])
        mw_col = find_col(['capacity_mw', 'mw', 'capacity', 'nameplate'])
        name_col = find_col(['project_name', 'project', 'name'])
        id_col = find_col(['queue_id', 'request_id', 'queue_pos', 'position'])
        state_col = find_col(['state'])
        county_col = find_col(['county'])
        status_col = find_col(['queue_status', 'status'])
        fuel_col = find_col(['resource_type', 'resource', 'fuel', 'type', 'technology'])
        developer_col = find_col(['developer', 'interconnection', 'owner', 'applicant'])
        
        for idx, row in df.iterrows():
            try:
                entity = str(row.get(entity_col, '') if entity_col else '').upper()
                
                # Map entity to utility name
                if 'PJM' in entity:
                    utility = 'PJM'
                elif 'MISO' in entity:
                    utility = 'MISO'
                elif 'CAISO' in entity or 'CALIFORNIA' in entity:
                    utility = 'CAISO'
                elif 'ERCOT' in entity or 'TEXAS' in entity:
                    utility = 'ERCOT'
                elif 'SPP' in entity:
                    utility = 'SPP'
                elif 'NYISO' in entity or 'NEW YORK' in entity:
                    utility = 'NYISO'
                elif 'ISO-NE' in entity or 'ISONE' in entity or 'NEW ENGLAND' in entity:
                    utility = 'ISO-NE'
                else:
                    utility = entity[:20] if entity else 'Other'
                
                capacity = self.extract_capacity(row.get(mw_col, 0) if mw_col else 0)
                if not capacity:
                    continue
                
                proj = {
                    'request_id': f"{utility}_BL_{row.get(id_col, idx) if id_col else idx}",
                    'project_name': str(row.get(name_col, 'Unknown') if name_col else 'Unknown')[:500],
                    'capacity_mw': capacity,
                    'county': str(row.get(county_col, '') if county_col else '')[:200],
                    'state': str(row.get(state_col, '') if state_col else '')[:2],
                    'customer': str(row.get(developer_col, '') if developer_col else '')[:500],
                    'utility': utility,
                    'status': str(row.get(status_col, 'Active') if status_col else 'Active'),
                    'fuel_type': str(row.get(fuel_col, '') if fuel_col else ''),
                    'source': f'{utility} (Berkeley Lab)',
                    'source_url': successful_url,
                    'project_type': self.classify_project(
                        str(row.get(name_col, '') if name_col else ''),
                        str(row.get(developer_col, '') if developer_col else ''),
                        str(row.get(fuel_col, '') if fuel_col else '')
                    )
                }
                proj['hunter_score'] = self.calculate_hunter_score(proj)
                proj['data_hash'] = self.generate_hash(proj)
                projects.append(proj)
                
            except Exception as e:
                continue
        
        # Cache by utility
        for proj in projects:
            utility = proj.get('utility', 'Other')
            if utility not in self.berkeley_lab_cache:
                self.berkeley_lab_cache[utility] = []
            self.berkeley_lab_cache[utility].append(proj)
        
        logger.info(f"Berkeley Lab: Extracted {len(projects)} total projects")
        
        # Log breakdown
        breakdown = {}
        for p in projects:
            u = p.get('utility', 'Other')
            breakdown[u] = breakdown.get(u, 0) + 1
        logger.info(f"Berkeley Lab breakdown: {breakdown}")
        
        return projects

    # =========================================================================
    # Main Run
    # =========================================================================
    def run_comprehensive_monitoring(self):
        """Run all monitors and store results"""
        start_time = time.time()
        
        monitors = [
            ('CAISO', self.fetch_caiso),
            ('NYISO', self.fetch_nyiso),
            ('ISO-NE', self.fetch_isone),
            ('SPP', self.fetch_spp),
            ('MISO', self.fetch_miso),
            ('ERCOT', self.fetch_ercot),
            ('PJM', self.fetch_pjm),
        ]
        
        all_projects = []
        stats = {}
        
        for source_name, fetch_func in monitors:
            try:
                logger.info(f"Fetching {source_name}...")
                projects = fetch_func()
                all_projects.extend(projects)
                stats[source_name] = len(projects)
                logger.info(f"{source_name}: {len(projects)} projects")
                
                db.execute('''
                    INSERT INTO sync_log (source, projects_found, projects_new, status)
                    VALUES (?, ?, 0, 'success')
                ''', (source_name, len(projects)))
                
            except Exception as e:
                logger.error(f"{source_name} failed: {e}")
                stats[source_name] = 0
                db.execute('''
                    INSERT INTO sync_log (source, projects_found, projects_new, status, error_message)
                    VALUES (?, 0, 0, 'error', ?)
                ''', (source_name, str(e)))
        
        # Store projects
        new_count = 0
        for project in all_projects:
            try:
                existing = db.fetchone('SELECT id FROM projects WHERE request_id = ?', (project['request_id'],))
                if existing:
                    db.execute('''
                        UPDATE projects SET
                            project_name=?, capacity_mw=?, county=?, state=?, customer=?,
                            utility=?, status=?, fuel_type=?, source=?, source_url=?,
                            project_type=?, hunter_score=?, data_hash=?, last_updated=CURRENT_TIMESTAMP
                        WHERE request_id=?
                    ''', (
                        project['project_name'], project['capacity_mw'], project.get('county', ''),
                        project.get('state', ''), project.get('customer', ''), project['utility'],
                        project.get('status', ''), project.get('fuel_type', ''), project['source'],
                        project.get('source_url', ''), project.get('project_type', ''),
                        project.get('hunter_score', 0), project['data_hash'], project['request_id']
                    ))
                else:
                    db.execute('''
                        INSERT INTO projects (request_id, project_name, capacity_mw, county, state,
                            customer, utility, status, fuel_type, source, source_url, project_type,
                            hunter_score, data_hash)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        project['request_id'], project['project_name'], project['capacity_mw'],
                        project.get('county', ''), project.get('state', ''), project.get('customer', ''),
                        project['utility'], project.get('status', ''), project.get('fuel_type', ''),
                        project['source'], project.get('source_url', ''), project.get('project_type', ''),
                        project.get('hunter_score', 0), project['data_hash']
                    ))
                    new_count += 1
            except Exception as e:
                logger.debug(f"Failed to store project: {e}")
        
        duration = time.time() - start_time
        
        # Log run
        db.execute('''
            INSERT INTO monitor_runs (status, sources_checked, projects_found, projects_stored, duration_seconds, details)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', ('success', len(monitors), len(all_projects), new_count, duration, json.dumps(stats)))
        
        logger.info(f"Monitoring complete: {len(all_projects)} projects, {new_count} new, {duration:.1f}s")
        
        return {
            'total': len(all_projects),
            'new': new_count,
            'by_source': stats,
            'duration': round(duration, 2)
        }


# Initialize monitor
monitor = HybridPowerMonitor(min_capacity_mw=100)


# =============================================================================
# Flask Routes
# =============================================================================

@app.route('/')
def index():
    """Dashboard home"""
    total = db.fetchone('SELECT COUNT(*) as count FROM projects')['count']
    total_mw = db.fetchone('SELECT SUM(capacity_mw) as total FROM projects')['total'] or 0
    
    by_utility = db.fetchall('''
        SELECT utility, COUNT(*) as count, SUM(capacity_mw) as total_mw
        FROM projects GROUP BY utility ORDER BY count DESC
    ''')
    
    by_type = db.fetchall('''
        SELECT project_type, COUNT(*) as count
        FROM projects GROUP BY project_type ORDER BY count DESC
    ''')
    
    high_score = db.fetchone('SELECT COUNT(*) as count FROM projects WHERE hunter_score >= 60')['count']
    recent = db.fetchall('''
        SELECT * FROM projects ORDER BY first_seen DESC LIMIT 10
    ''')
    
    last_run = db.fetchone('SELECT * FROM monitor_runs ORDER BY run_date DESC LIMIT 1')
    
    return render_template('index.html',
        total=total,
        total_mw=total_mw,
        by_utility=by_utility,
        by_type=by_type,
        high_score=high_score,
        recent=recent,
        last_run=last_run,
        gridstatus_available=GRIDSTATUS_AVAILABLE
    )


@app.route('/projects')
def projects():
    """Projects list with filtering"""
    page = request.args.get('page', 1, type=int)
    per_page = 50
    filter_type = request.args.get('filter', 'all')
    state_filter = request.args.get('state', '')
    min_capacity = request.args.get('min_capacity', type=int)
    search = request.args.get('search', '')
    
    # Build query
    conditions = []
    params = []
    
    if filter_type == 'hunter':
        conditions.append('hunter_score >= 60')
    elif filter_type == 'datacenter':
        conditions.append("project_type = 'datacenter'")
    elif filter_type == 'load':
        conditions.append('capacity_mw >= 100')
    
    if state_filter:
        conditions.append('state = ?')
        params.append(state_filter)
    
    if min_capacity:
        conditions.append('capacity_mw >= ?')
        params.append(min_capacity)
    
    if search:
        conditions.append('(project_name LIKE ? OR customer LIKE ? OR county LIKE ?)')
        params.extend([f'%{search}%'] * 3)
    
    where_clause = ' AND '.join(conditions) if conditions else '1=1'
    
    # Get total count
    total = db.fetchone(f'SELECT COUNT(*) as count FROM projects WHERE {where_clause}', params)['count']
    
    # Get paginated results
    offset = (page - 1) * per_page
    query_params = params + [per_page, offset]
    items = db.fetchall(f'''
        SELECT * FROM projects WHERE {where_clause}
        ORDER BY hunter_score DESC, capacity_mw DESC
        LIMIT ? OFFSET ?
    ''', query_params)
    
    # Get states for filter
    states = [r['state'] for r in db.fetchall('SELECT DISTINCT state FROM projects WHERE state != "" ORDER BY state')]
    
    # Create pagination object
    class Pagination:
        def __init__(self, items, page, per_page, total):
            self.items = items
            self.page = page
            self.per_page = per_page
            self.total = total
            self.pages = (total + per_page - 1) // per_page
            self.has_prev = page > 1
            self.has_next = page < self.pages
            self.prev_num = page - 1
            self.next_num = page + 1
    
    pagination = Pagination(items, page, per_page, total)
    
    return render_template('projects.html',
        pagination=pagination,
        filter_type=filter_type,
        state_filter=state_filter,
        min_capacity=min_capacity,
        search=search,
        states=states
    )


@app.route('/project/<int:id>')
def project_detail(id):
    """Single project detail"""
    project = db.fetchone('SELECT * FROM projects WHERE id = ?', (id,))
    if not project:
        return redirect(url_for('projects'))
    return render_template('project_detail.html', project=project)


@app.route('/analytics')
def analytics():
    """Analytics dashboard"""
    # Score distribution
    high = db.fetchone('SELECT COUNT(*) as count FROM projects WHERE hunter_score >= 70')['count']
    medium = db.fetchone('SELECT COUNT(*) as count FROM projects WHERE hunter_score >= 40 AND hunter_score < 70')['count']
    low = db.fetchone('SELECT COUNT(*) as count FROM projects WHERE hunter_score < 40')['count']
    
    score_distribution = {'high': high, 'medium': medium, 'low': low}
    
    # State stats
    state_stats = db.fetchall('''
        SELECT state, COUNT(*) as count FROM projects 
        WHERE state != '' GROUP BY state ORDER BY count DESC LIMIT 15
    ''')
    
    # Hotspot stats
    hotspot_stats = db.fetchall('''
        SELECT county, state, COUNT(*) as count, AVG(hunter_score) as avg_score
        FROM projects WHERE county != ''
        GROUP BY county, state ORDER BY count DESC LIMIT 15
    ''')
    
    # Timeline (last 30 days)
    timeline = db.fetchall('''
        SELECT DATE(first_seen) as date, COUNT(*) as count
        FROM projects WHERE first_seen >= DATE('now', '-30 days')
        GROUP BY DATE(first_seen) ORDER BY date
    ''')
    
    return render_template('analytics.html',
        score_distribution=score_distribution,
        state_stats=state_stats,
        hotspot_stats=hotspot_stats,
        timeline=timeline
    )


@app.route('/monitoring')
def monitoring():
    """System monitoring page"""
    runs = db.fetchall('SELECT * FROM monitor_runs ORDER BY run_date DESC LIMIT 20')
    source_stats = db.fetchall('''
        SELECT source, MAX(sync_time) as last_sync, SUM(projects_found) as total_found
        FROM sync_log GROUP BY source
    ''')
    
    return render_template('monitoring.html', runs=runs, source_stats=source_stats)


@app.route('/alerts', methods=['GET', 'POST'])
def alerts():
    """Alert subscriptions"""
    subscriptions = db.fetchall('SELECT * FROM alert_subscriptions WHERE active = 1')
    return render_template('alerts.html', subscriptions=subscriptions)


@app.route('/subscribe', methods=['POST'])
def subscribe_alerts():
    """Subscribe to alerts"""
    email = request.form.get('email')
    min_capacity = request.form.get('min_capacity', 200, type=int)
    states = ','.join(request.form.getlist('states'))
    
    try:
        db.execute('''
            INSERT OR REPLACE INTO alert_subscriptions (email, min_capacity, states, active)
            VALUES (?, ?, ?, 1)
        ''', (email, min_capacity, states))
    except Exception as e:
        logger.error(f"Subscribe failed: {e}")
    
    return redirect(url_for('alerts'))


@app.route('/trigger')
def trigger_monitor():
    """Trigger manual sync"""
    result = monitor.run_comprehensive_monitoring()
    return redirect(url_for('monitoring'))


@app.route('/export')
def export_csv():
    """Export projects to CSV"""
    min_score = request.args.get('min_score', 0, type=int)
    
    projects = db.fetchall('''
        SELECT request_id, project_name, capacity_mw, county, state, customer,
               utility, status, fuel_type, project_type, hunter_score, first_seen
        FROM projects WHERE hunter_score >= ?
        ORDER BY hunter_score DESC, capacity_mw DESC
    ''', (min_score,))
    
    # Build CSV
    lines = ['Request ID,Project Name,Capacity MW,County,State,Customer,Utility,Status,Fuel Type,Type,Score,First Seen']
    for p in projects:
        line = ','.join([
            f'"{p["request_id"]}"',
            f'"{(p["project_name"] or "").replace(chr(34), chr(39))}"',
            str(p['capacity_mw']),
            f'"{p["county"] or ""}"',
            f'"{p["state"] or ""}"',
            f'"{(p["customer"] or "").replace(chr(34), chr(39))}"',
            f'"{p["utility"]}"',
            f'"{p["status"] or ""}"',
            f'"{p["fuel_type"] or ""}"',
            f'"{p["project_type"] or ""}"',
            str(p['hunter_score']),
            f'"{p["first_seen"]}"'
        ])
        lines.append(line)
    
    csv_content = '\n'.join(lines)
    
    return Response(
        csv_content,
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename=power_projects_{datetime.now().strftime("%Y%m%d")}.csv'}
    )


# =============================================================================
# API Routes
# =============================================================================

@app.route('/api/stats')
def api_stats():
    """API: Get statistics"""
    total = db.fetchone('SELECT COUNT(*) as count FROM projects')['count']
    by_utility = [dict(r) for r in db.fetchall('''
        SELECT utility, COUNT(*) as count, SUM(capacity_mw) as total_mw
        FROM projects GROUP BY utility
    ''')]
    by_state = [dict(r) for r in db.fetchall('''
        SELECT state, COUNT(*) as count FROM projects WHERE state != '' GROUP BY state
    ''')]
    
    return jsonify({
        'total_projects': total,
        'by_utility': by_utility,
        'by_state': by_state,
        'gridstatus_available': GRIDSTATUS_AVAILABLE
    })


@app.route('/api/projects')
def api_projects():
    """API: Get projects"""
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    min_score = request.args.get('min_score', 0, type=int)
    
    projects = db.fetchall('''
        SELECT * FROM projects WHERE hunter_score >= ?
        ORDER BY hunter_score DESC, capacity_mw DESC
        LIMIT ? OFFSET ?
    ''', (min_score, limit, offset))
    
    return jsonify([dict(p) for p in projects])


@app.route('/api/sync', methods=['POST'])
def api_sync():
    """API: Trigger sync"""
    result = monitor.run_comprehensive_monitoring()
    return jsonify(result)


# =============================================================================
# Startup
# =============================================================================

def init_app():
    """Initialize application"""
    os.makedirs(DATA_DIR, exist_ok=True)
    logger.info(f"Power Monitor v{APP_VERSION} starting. gridstatus: {GRIDSTATUS_AVAILABLE}")
    
    # Check if we need initial sync
    count = db.fetchone('SELECT COUNT(*) as count FROM projects')['count']
    if count == 0:
        logger.info("No projects in database, running initial sync...")
        try:
            monitor.run_comprehensive_monitoring()
        except Exception as e:
            logger.error(f"Initial sync failed: {e}")


init_app()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
