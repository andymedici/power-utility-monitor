# app.py - Hybrid Power Monitor using gridstatus and direct URLs
import os
import sys
import logging
import requests
import pandas as pd
import re
import hashlib
import time
import json
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from bs4 import BeautifulSoup

# Fix Railway PostgreSQL URL
if 'DATABASE_URL' in os.environ:
    if os.environ['DATABASE_URL'].startswith('postgres://'):
        os.environ['DATABASE_URL'] = os.environ['DATABASE_URL'].replace('postgres://', 'postgresql://', 1)

from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy

# Try to import gridstatus - will use direct URLs if not available
try:
    import gridstatus
    GRIDSTATUS_AVAILABLE = True
except ImportError:
    GRIDSTATUS_AVAILABLE = False
    logging.warning("gridstatus not installed - using direct URL fetching only")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Database Models
class PowerProject(db.Model):
    __tablename__ = 'power_projects'
    
    id = db.Column(db.Integer, primary_key=True)
    request_id = db.Column(db.String(255), unique=True, index=True)
    queue_position = db.Column(db.String(100))
    project_name = db.Column(db.String(500))
    capacity_mw = db.Column(db.Float, index=True)
    location = db.Column(db.String(500))
    county = db.Column(db.String(200))
    state = db.Column(db.String(2), index=True)
    customer = db.Column(db.String(500))
    developer = db.Column(db.String(500))
    utility = db.Column(db.String(255))
    interconnection_point = db.Column(db.String(500))
    project_type = db.Column(db.String(50), index=True)
    fuel_type = db.Column(db.String(100))
    status = db.Column(db.String(100))
    queue_date = db.Column(db.Date)
    in_service_date = db.Column(db.Date)
    withdrawal_date = db.Column(db.Date)
    
    source = db.Column(db.String(100), index=True)
    source_url = db.Column(db.Text)
    data_hash = db.Column(db.String(32), unique=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    @classmethod
    def active(cls):
        """Return only projects from last 90 days"""
        cutoff = datetime.utcnow() - timedelta(days=90)
        return cls.query.filter(cls.created_at >= cutoff)

class MonitoringRun(db.Model):
    __tablename__ = 'monitoring_runs'
    
    id = db.Column(db.Integer, primary_key=True)
    run_date = db.Column(db.DateTime, default=datetime.utcnow)
    sources_checked = db.Column(db.Integer, default=0)
    projects_found = db.Column(db.Integer, default=0)
    projects_stored = db.Column(db.Integer, default=0)
    duration_seconds = db.Column(db.Float)
    status = db.Column(db.String(50))
    details = db.Column(db.Text)

class BerkeleyLabSync(db.Model):
    __tablename__ = 'berkeley_lab_syncs'
    
    id = db.Column(db.Integer, primary_key=True)
    sync_date = db.Column(db.DateTime, default=datetime.utcnow)
    file_url = db.Column(db.String(500))
    projects_found = db.Column(db.Integer, default=0)
    projects_stored = db.Column(db.Integer, default=0)
    duration_seconds = db.Column(db.Float)
    status = db.Column(db.String(50))
    error_message = db.Column(db.Text)
    next_sync_date = db.Column(db.DateTime)

# Hybrid Power Monitor - gridstatus + direct URLs
class HybridPowerMonitor:
    def __init__(self):
        self.min_capacity_mw = 100
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def extract_capacity(self, value):
        """Extract MW capacity from various formats"""
        if pd.isna(value) or not value:
            return None
        
        text = str(value).replace(',', '').strip()
        
        try:
            capacity = float(text)
            if capacity >= self.min_capacity_mw:
                return capacity
        except:
            pass
        
        return None
    
    def generate_hash(self, data):
        """Generate unique hash for duplicate detection"""
        key = f"{data.get('project_name', '')}_{data.get('capacity_mw', 0)}_{data.get('location', '')}_{data.get('source', '')}"
        return hashlib.md5(key.lower().encode()).hexdigest()
    
    def classify_project(self, name, customer='', fuel_type=''):
        """Classify project type"""
        text = f"{name} {customer} {fuel_type}".lower()
        
        if any(word in text for word in ['data center', 'datacenter', 'cloud', 'hyperscale']):
            return 'datacenter'
        if any(word in text for word in ['battery', 'storage', 'bess']):
            return 'storage'
        if any(word in text for word in ['solar', 'photovoltaic', 'pv']):
            return 'solar'
        if any(word in text for word in ['wind']):
            return 'wind'
        if any(word in text for word in ['manufacturing', 'factory']):
            return 'manufacturing'
        
        return 'other'
    
    def fetch_caiso_gridstatus(self):
        """Try CAISO using gridstatus first"""
        projects = []
        
        if GRIDSTATUS_AVAILABLE:
            try:
                logger.info("CAISO: Attempting gridstatus fetch")
                caiso = gridstatus.CAISO()
                df = caiso.get_interconnection_queue()
                
                # gridstatus standardized columns
                for _, row in df.iterrows():
                    capacity = self.extract_capacity(row.get('Capacity (MW)', row.get('capacity_mw', 0)))
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        data = {
                            'request_id': f"CAISO_{row.get('Queue ID', row.get('queue_id', 'UNK'))}",
                            'queue_position': str(row.get('Queue Position', '')),
                            'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': str(row.get('State', 'CA'))[:2],
                            'customer': str(row.get('Interconnection Customer', ''))[:500],
                            'utility': 'CAISO',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Fuel', '')),
                            'source': 'CAISO',
                            'source_url': 'http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx',
                            'project_type': self.classify_project(
                                row.get('Project Name', ''),
                                row.get('Interconnection Customer', ''),
                                row.get('Fuel', '')
                            )
                        }
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                
                logger.info(f"CAISO gridstatus: Found {len(projects)} projects")
                return projects
            except Exception as e:
                logger.error(f"CAISO gridstatus failed: {e}, trying direct URL")
        
        # Fallback to direct URL
        return self.fetch_caiso_direct()
    
    def fetch_caiso_direct(self):
        """CAISO direct URL fallback"""
        projects = []
        excel_url = 'http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx'
        
        try:
            logger.info(f"CAISO: Direct fetch from {excel_url}")
            response = self.session.get(excel_url, timeout=30, verify=False)
            
            if response.status_code == 200:
                df = pd.read_excel(BytesIO(response.content))
                logger.info(f"CAISO: Processing {len(df)} rows")
                
                # Find MW columns dynamically
                mw_cols = [col for col in df.columns if 'MW' in str(col).upper()]
                logger.info(f"CAISO MW columns: {mw_cols}")
                
                for _, row in df.iterrows():
                    capacity = None
                    for col in mw_cols + ['Capacity', 'Max Output']:
                        if col in df.columns:
                            capacity = self.extract_capacity(row.get(col))
                            if capacity:
                                break
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        data = {
                            'request_id': f"CAISO_{row.get('Queue Number', row.get('Request Number', 'UNK'))}",
                            'project_name': str(row.get('Project Name', row.get('Generating Facility', 'Unknown')))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': 'CA',
                            'customer': str(row.get('Interconnection Customer', ''))[:500],
                            'utility': 'CAISO',
                            'status': str(row.get('Status', 'Active')),
                            'source': 'CAISO',
                            'source_url': excel_url,
                            'project_type': 'other'
                        }
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"CAISO direct fetch error: {e}")
        
        logger.info(f"CAISO direct: Found {len(projects)} projects")
        return projects
    
    def fetch_nyiso_direct(self):
        """NYISO - always use direct URL (it works)"""
        projects = []
        excel_url = 'https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx'
        
        try:
            logger.info(f"NYISO: Fetching from {excel_url}")
            response = self.session.get(excel_url, timeout=30, verify=False)
            
            if response.status_code == 200:
                df = pd.read_excel(BytesIO(response.content))
                logger.info(f"NYISO: Processing {len(df)} rows")
                
                # Find MW columns
                mw_cols = [col for col in df.columns if 'MW' in str(col).upper()]
                logger.info(f"NYISO MW columns: {mw_cols}")
                
                for _, row in df.iterrows():
                    capacity = None
                    for col in mw_cols + ['Capacity']:
                        if col in df.columns:
                            capacity = self.extract_capacity(row.get(col))
                            if capacity:
                                break
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        data = {
                            'request_id': f"NYISO_{row.get('Queue Pos.', row.get('Queue Position', 'UNK'))}",
                            'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': 'NY',
                            'customer': str(row.get('Developer', ''))[:500],
                            'utility': 'NYISO',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Type', '')),
                            'source': 'NYISO',
                            'source_url': excel_url,
                            'project_type': self.classify_project(
                                row.get('Project Name', ''),
                                row.get('Developer', ''),
                                row.get('Type', '')
                            )
                        }
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"NYISO error: {e}")
        
        logger.info(f"NYISO: Found {len(projects)} projects")
        return projects
    
    def fetch_isone(self):
        """ISO-NE - fetch from IRTT public queue HTML table"""
        projects = []
        html_url = 'https://irtt.iso-ne.com/reports/external'
        
        try:
            logger.info(f"ISO-NE: Fetching from {html_url}")
            response = self.session.get(html_url, timeout=30)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find the main data table
                table = soup.find('table')
                if not table:
                    logger.error("ISO-NE: Could not find data table in HTML")
                    return projects
                
                # Get headers
                headers = []
                header_row = table.find('thead')
                if header_row:
                    headers = [th.get_text(strip=True) for th in header_row.find_all('th')]
                logger.info(f"ISO-NE: Found {len(headers)} columns")
                
                # Parse data rows
                tbody = table.find('tbody')
                if not tbody:
                    logger.error("ISO-NE: Could not find tbody in table")
                    return projects
                
                rows = tbody.find_all('tr')
                logger.info(f"ISO-NE: Processing {len(rows)} rows")
                
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) < 10:  # Skip incomplete rows
                        continue
                    
                    try:
                        # Create a dict mapping headers to cell values
                        row_data = {}
                        for i, cell in enumerate(cells):
                            if i < len(headers):
                                row_data[headers[i]] = cell.get_text(strip=True)
                        
                        # Extract capacity - try multiple MW columns
                        capacity = None
                        for mw_col in ['Net MW', 'Summer MW', 'Winter MW']:
                            if mw_col in row_data:
                                capacity = self.extract_capacity(row_data.get(mw_col))
                                if capacity:
                                    break
                        
                        if not capacity or capacity < self.min_capacity_mw:
                            continue
                        
                        # Extract other fields
                        queue_pos = row_data.get('QP', '')
                        project_name = row_data.get('Alternative Name', row_data.get('Unit', 'Unknown'))
                        county = row_data.get('County', '')
                        state = row_data.get('ST', 'MA')  # Default to MA if not specified
                        fuel_type = row_data.get('Fuel Type', '')
                        status = row_data.get('Status', 'Active')
                        
                        data = {
                            'request_id': f"ISONE_{queue_pos}" if queue_pos else f"ISONE_{project_name[:20]}",
                            'queue_position': queue_pos,
                            'project_name': str(project_name)[:500],
                            'capacity_mw': capacity,
                            'county': str(county)[:200],
                            'state': str(state)[:2] if state else 'MA',
                            'customer': '',  # Not available in this table
                            'utility': 'ISO-NE',
                            'status': str(status),
                            'fuel_type': str(fuel_type),
                            'source': 'ISO-NE',
                            'source_url': html_url,
                            'project_type': self.classify_project(
                                project_name,
                                '',
                                fuel_type
                            )
                        }
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
                    except Exception as e:
                        logger.error(f"ISO-NE: Error parsing row: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"ISO-NE error: {e}")
        
        logger.info(f"ISO-NE: Found {len(projects)} projects")
        return projects
    
    def fetch_berkeley_lab(self):
        """Berkeley Lab - comprehensive quarterly data for ALL ISOs"""
        projects = []
        
        # Berkeley Lab publishes data quarterly/annually
        # Try multiple URL patterns as they sometimes change
        possible_urls = [
            'https://emp.lbl.gov/sites/default/files/queued_up_2025_data_file.xlsx',
            'https://emp.lbl.gov/sites/default/files/lbnl_interconnection_queue_2025.xlsx',
            'https://emp.lbl.gov/sites/default/files/2025/queued_up_data.xlsx',
        ]
        
        excel_data = None
        successful_url = None
        
        for url in possible_urls:
            try:
                logger.info(f"Berkeley Lab: Trying {url}")
                response = self.session.get(url, timeout=60)
                
                if response.status_code == 200 and len(response.content) > 1000:
                    excel_data = BytesIO(response.content)
                    successful_url = url
                    logger.info(f"Berkeley Lab: Successfully downloaded from {url}")
                    break
            except Exception as e:
                logger.warning(f"Berkeley Lab: Failed to fetch from {url}: {e}")
                continue
        
        if not excel_data:
            logger.error("Berkeley Lab: Could not download data file from any known URL")
            return projects
        
        try:
            # Berkeley Lab file has multiple sheets - we want 'active_projects' or similar
            df = pd.read_excel(excel_data, sheet_name=0, engine='openpyxl')
            logger.info(f"Berkeley Lab: Loaded sheet with {len(df)} rows")
            logger.info(f"Berkeley Lab: Columns: {list(df.columns)[:10]}")
            
            # Focus on ISOs we don't have real-time data for: MISO, ERCOT, PJM
            target_entities = ['MISO', 'ERCOT', 'PJM']
            
            for _, row in df.iterrows():
                try:
                    # Berkeley Lab uses standardized column names
                    entity = str(row.get('entity', row.get('Entity', '')))
                    
                    # Only process ISOs we don't have real-time data for
                    if entity not in target_entities:
                        continue
                    
                    # Extract capacity
                    capacity = self.extract_capacity(
                        row.get('capacity_mw_resource', 
                        row.get('Capacity (MW)', 
                        row.get('capacity_mw', 0)))
                    )
                    
                    if not capacity or capacity < self.min_capacity_mw:
                        continue
                    
                    # Extract fields
                    queue_id = str(row.get('queue_id', row.get('Queue ID', '')))
                    project_name = str(row.get('project_name', row.get('Project Name', 'Unknown')))[:500]
                    county = str(row.get('county', row.get('County', '')))[:200]
                    state = str(row.get('state', row.get('State', '')))[:2]
                    developer = str(row.get('developer', row.get('Developer', '')))[:500]
                    fuel_type = str(row.get('resource_type_primary', row.get('Fuel Type', '')))
                    status = str(row.get('queue_status', row.get('Status', 'Active')))
                    
                    # Only include active projects
                    if 'withdraw' in status.lower() or 'inactive' in status.lower():
                        continue
                    
                    data = {
                        'request_id': f"{entity}_{queue_id}" if queue_id else f"{entity}_{project_name[:20]}",
                        'queue_position': queue_id,
                        'project_name': project_name,
                        'capacity_mw': capacity,
                        'county': county,
                        'state': state,
                        'customer': developer,
                        'developer': developer,
                        'utility': entity,
                        'status': status,
                        'fuel_type': fuel_type,
                        'source': f'{entity} (Berkeley Lab)',
                        'source_url': successful_url,
                        'project_type': self.classify_project(
                            project_name,
                            developer,
                            fuel_type
                        )
                    }
                    
                    data['data_hash'] = self.generate_hash(data)
                    projects.append(data)
                    
                except Exception as e:
                    logger.error(f"Berkeley Lab: Error parsing row: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Berkeley Lab: Error processing Excel file: {e}")
        
        logger.info(f"Berkeley Lab: Found {len(projects)} projects from {target_entities}")
        return projects
    
    def should_run_berkeley_lab_sync(self):
        """Check if it's time to sync Berkeley Lab data (monthly)"""
        last_sync = BerkeleyLabSync.query.order_by(BerkeleyLabSync.sync_date.desc()).first()
        
        if not last_sync:
            return True  # Never synced before
        
        # Run if last sync was more than 30 days ago
        days_since_sync = (datetime.utcnow() - last_sync.sync_date).days
        return days_since_sync >= 30
    
    def run_berkeley_lab_sync(self):
        """Run Berkeley Lab sync and record results"""
        start_time = time.time()
        
        try:
            projects = self.fetch_berkeley_lab()
            
            # Store projects
            stored = 0
            for project_data in projects:
                try:
                    existing = PowerProject.query.filter_by(data_hash=project_data['data_hash']).first()
                    if not existing:
                        project = PowerProject(**project_data)
                        db.session.add(project)
                        stored += 1
                except Exception as e:
                    logger.error(f"Error storing Berkeley Lab project: {e}")
            
            db.session.commit()
            
            duration = time.time() - start_time
            
            # Record sync
            next_sync = datetime.utcnow() + timedelta(days=30)
            sync_record = BerkeleyLabSync(
                file_url='Berkeley Lab Queued Up data',
                projects_found=len(projects),
                projects_stored=stored,
                duration_seconds=duration,
                status='completed',
                next_sync_date=next_sync
            )
            db.session.add(sync_record)
            db.session.commit()
            
            logger.info(f"Berkeley Lab sync complete: {stored} new projects stored")
            return {
                'projects_found': len(projects),
                'projects_stored': stored,
                'duration': round(duration, 2),
                'next_sync': next_sync
            }
            
        except Exception as e:
            logger.error(f"Berkeley Lab sync error: {e}")
            
            sync_record = BerkeleyLabSync(
                status='failed',
                error_message=str(e),
                next_sync_date=datetime.utcnow() + timedelta(days=1)  # Retry tomorrow
            )
            db.session.add(sync_record)
            db.session.commit()
            
            return {
                'projects_found': 0,
                'projects_stored': 0,
                'error': str(e)
            }
    
    def fetch_ercot_gridstatus(self):
        """Try ERCOT using gridstatus"""
        projects = []
        
        if GRIDSTATUS_AVAILABLE:
            try:
                logger.info("ERCOT: Attempting gridstatus fetch")
                ercot = gridstatus.ERCOT()
                df = ercot.get_interconnection_queue()
                
                for _, row in df.iterrows():
                    capacity = self.extract_capacity(row.get('Capacity (MW)', row.get('capacity_mw', 0)))
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        data = {
                            'request_id': f"ERCOT_{row.get('Queue ID', 'UNK')}",
                            'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': 'TX',
                            'customer': str(row.get('Company', ''))[:500],
                            'utility': 'ERCOT',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Fuel', '')),
                            'source': 'ERCOT',
                            'source_url': 'https://www.ercot.com',
                            'project_type': self.classify_project(
                                row.get('Project Name', ''),
                                row.get('Company', ''),
                                row.get('Fuel', '')
                            )
                        }
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                
                logger.info(f"ERCOT gridstatus: Found {len(projects)} projects")
            except Exception as e:
                logger.error(f"ERCOT gridstatus failed: {e}")
        
        return projects
    
    def fetch_spp_direct(self):
        """SPP - try direct CSV URL"""
        projects = []
        csv_url = 'https://opsportal.spp.org/Studies/GenerateActiveCSV'
        
        try:
            logger.info(f"SPP: Fetching from {csv_url}")
            response = self.session.get(csv_url, timeout=30, verify=False)
            
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text))
                logger.info(f"SPP: Processing {len(df)} rows")
                
                # Find MW columns
                mw_cols = [col for col in df.columns if 'MW' in str(col).upper()]
                logger.info(f"SPP MW columns: {mw_cols}")
                
                for _, row in df.iterrows():
                    capacity = None
                    for col in mw_cols + ['Size', 'Capacity']:
                        if col in df.columns:
                            capacity = self.extract_capacity(row.get(col))
                            if capacity:
                                break
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        data = {
                            'request_id': f"SPP_{row.get('Request Number', row.get('GEN-', 'UNK'))}",
                            'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                            'capacity_mw': capacity,
                            'state': str(row.get('State', ''))[:2],
                            'customer': str(row.get('Customer', ''))[:500],
                            'utility': 'SPP',
                            'status': str(row.get('Status', 'Active')),
                            'source': 'SPP',
                            'source_url': csv_url,
                            'project_type': 'other'
                        }
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"SPP error: {e}")
        
        logger.info(f"SPP: Found {len(projects)} projects")
        return projects
    
    def fetch_pjm_gridstatus(self):
        """Try PJM using gridstatus if API key available"""
        projects = []
        
        if GRIDSTATUS_AVAILABLE and os.getenv('PJM_API_KEY'):
            try:
                logger.info("PJM: Attempting gridstatus fetch with API key")
                pjm = gridstatus.PJM(api_key=os.getenv('PJM_API_KEY'))
                df = pjm.get_interconnection_queue()
                
                for _, row in df.iterrows():
                    capacity = self.extract_capacity(row.get('Capacity (MW)', row.get('capacity_mw', 0)))
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        data = {
                            'request_id': f"PJM_{row.get('Queue ID', 'UNK')}",
                            'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': str(row.get('State', ''))[:2],
                            'utility': 'PJM',
                            'status': str(row.get('Status', 'Active')),
                            'source': 'PJM',
                            'source_url': 'https://www.pjm.com',
                            'project_type': 'other'
                        }
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                
                logger.info(f"PJM gridstatus: Found {len(projects)} projects")
            except Exception as e:
                logger.error(f"PJM gridstatus failed: {e}")
        else:
            logger.info("PJM: Skipping - no API key available")
        
        return projects
    
    def run_comprehensive_monitoring(self):
        """Run monitoring using hybrid approach"""
        start_time = time.time()
        all_projects = []
        source_stats = {}
        
        # Check if Berkeley Lab monthly sync is due
        berkeley_lab_result = None
        if self.should_run_berkeley_lab_sync():
            logger.info("Berkeley Lab sync is due - running monthly update")
            berkeley_lab_result = self.run_berkeley_lab_sync()
            source_stats['Berkeley Lab (Monthly)'] = berkeley_lab_result.get('projects_stored', 0)
        else:
            last_sync = BerkeleyLabSync.query.order_by(BerkeleyLabSync.sync_date.desc()).first()
            if last_sync:
                days_since = (datetime.utcnow() - last_sync.sync_date).days
                logger.info(f"Berkeley Lab sync not due (last sync: {days_since} days ago)")
        
        # Define monitors - mix of gridstatus and direct
        monitors = [
            ('CAISO', self.fetch_caiso_gridstatus),  # Tries gridstatus, falls back to direct
            ('NYISO', self.fetch_nyiso_direct),      # Direct URL (works well)
            ('ISO-NE', self.fetch_isone),            # Direct HTML parsing (works well)
            ('ERCOT', self.fetch_ercot_gridstatus),  # gridstatus only (needs API)
            ('SPP', self.fetch_spp_direct),           # Direct CSV
            ('PJM', self.fetch_pjm_gridstatus),      # gridstatus with API key
        ]
        
        total_sources = len(monitors)
        
        for source_name, fetch_func in monitors:
            try:
                logger.info(f"Fetching from {source_name}...")
                source_projects = fetch_func()
                all_projects.extend(source_projects)
                source_stats[source_name] = len(source_projects)
                logger.info(f"{source_name}: Retrieved {len(source_projects)} projects")
            except Exception as e:
                logger.error(f"Failed to fetch {source_name}: {e}")
                source_stats[source_name] = 0
        
        # Store unique projects
        stored = 0
        for project_data in all_projects:
            try:
                if 'data_hash' not in project_data:
                    project_data['data_hash'] = self.generate_hash(project_data)
                
                existing = PowerProject.query.filter_by(data_hash=project_data['data_hash']).first()
                if not existing:
                    # Fill in missing fields
                    for field in ['queue_position', 'location', 'county', 'customer', 'developer', 
                                 'utility', 'status', 'fuel_type', 'interconnection_point']:
                        if field not in project_data:
                            project_data[field] = ''
                    
                    project = PowerProject(**project_data)
                    db.session.add(project)
                    stored += 1
            except Exception as e:
                logger.error(f"Error storing project: {e}")
        
        db.session.commit()
        
        # Clean up old projects (>90 days)
        cutoff_date = datetime.utcnow() - timedelta(days=90)
        old_count = PowerProject.query.filter(PowerProject.created_at < cutoff_date).delete()
        db.session.commit()
        
        duration = time.time() - start_time
        
        # Record monitoring run
        run = MonitoringRun(
            sources_checked=total_sources,
            projects_found=len(all_projects),
            projects_stored=stored,
            duration_seconds=duration,
            status='completed',
            details=json.dumps(source_stats)
        )
        db.session.add(run)
        db.session.commit()
        
        result = {
            'sources_checked': total_sources,
            'projects_found': len(all_projects),
            'projects_stored': stored,
            'duration_seconds': round(duration, 2),
            'by_source': source_stats,
            'old_projects_removed': old_count,
            'gridstatus_available': GRIDSTATUS_AVAILABLE
        }
        
        if berkeley_lab_result:
            result['berkeley_lab_sync'] = berkeley_lab_result
        
        return result

# Flask Routes
@app.route('/')
def index():
    total = PowerProject.active().count()
    recent = PowerProject.active().filter(
        PowerProject.created_at >= datetime.utcnow() - timedelta(days=30)
    ).count()
    
    datacenter_count = PowerProject.active().filter_by(project_type='datacenter').count()
    solar_count = PowerProject.active().filter_by(project_type='solar').count()
    wind_count = PowerProject.active().filter_by(project_type='wind').count()
    storage_count = PowerProject.active().filter_by(project_type='storage').count()
    
    high_capacity = PowerProject.active().filter(PowerProject.capacity_mw >= 200).count()
    
    states_covered = db.session.query(PowerProject.state).filter(
        PowerProject.created_at >= datetime.utcnow() - timedelta(days=90)
    ).distinct().count()
    
    last_run = MonitoringRun.query.order_by(MonitoringRun.run_date.desc()).first()
    
    from sqlalchemy import func
    cutoff = datetime.utcnow() - timedelta(days=90)
    top_states = db.session.query(
        PowerProject.state,
        func.count(PowerProject.id).label('count'),
        func.sum(PowerProject.capacity_mw).label('total_mw')
    ).filter(
        PowerProject.created_at >= cutoff
    ).group_by(PowerProject.state).order_by(func.count(PowerProject.id).desc()).limit(5).all()
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Power Projects Monitor - Hybrid Data Collection</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; background: #f0f2f5; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px 40px; }}
            .container {{ max-width: 1400px; margin: 0 auto; padding: 20px 40px; }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 20px; margin: 20px 0; }}
            .stat-card {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .stat-card h3 {{ margin: 0; font-size: 32px; color: #333; }}
            .stat-card p {{ margin: 5px 0 0 0; color: #666; font-size: 14px; }}
            .card {{ background: white; padding: 25px; margin: 20px 0; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .btn {{ padding: 12px 24px; background: #007bff; color: white; text-decoration: none; border-radius: 5px; display: inline-block; margin: 5px; }}
            .btn:hover {{ background: #0056b3; }}
            .btn-success {{ background: #28a745; }}
            .nav {{ background: #2c3e50; padding: 0; }}
            .nav a {{ color: white; padding: 15px 20px; display: inline-block; text-decoration: none; }}
            .nav a:hover {{ background: #34495e; }}
            .status {{ padding: 3px 8px; border-radius: 3px; font-size: 11px; }}
            .status.active {{ background: #d4edda; color: #155724; }}
            table {{ width: 100%; }}
            th {{ text-align: left; padding: 10px; border-bottom: 2px solid #dee2e6; }}
            td {{ padding: 10px; border-bottom: 1px solid #dee2e6; }}
        </style>
    </head>
    <body>
        <div class="nav">
            <a href="/">🏠 Dashboard</a>
            <a href="/projects">📊 Projects</a>
            <a href="/monitoring">📡 Monitoring</a>
            <a href="/berkeley-lab-sync">🏛️ Berkeley Lab</a>
            <a href="/status">🔧 System Status</a>
            <a href="/export/csv">📥 Export</a>
        </div>
        
        <div class="header">
            <h1>⚡ Power Projects Monitor</h1>
            <p>Hybrid data collection: gridstatus + direct URLs | 90-day retention</p>
            <p style="font-size: 12px;">{'✅ gridstatus available' if GRIDSTATUS_AVAILABLE else '⚠️ gridstatus not installed - using direct URLs only'}</p>
        </div>
        
        <div class="container">
            <div class="stats-grid">
                <div class="stat-card">
                    <h3>{total}</h3>
                    <p>Active Projects</p>
                </div>
                <div class="stat-card">
                    <h3>{states_covered}</h3>
                    <p>States</p>
                </div>
                <div class="stat-card">
                    <h3>{datacenter_count}</h3>
                    <p>Data Centers</p>
                </div>
                <div class="stat-card">
                    <h3>{solar_count}</h3>
                    <p>Solar</p>
                </div>
                <div class="stat-card">
                    <h3>{wind_count}</h3>
                    <p>Wind</p>
                </div>
                <div class="stat-card">
                    <h3>{storage_count}</h3>
                    <p>Storage</p>
                </div>
                <div class="stat-card">
                    <h3>{high_capacity}</h3>
                    <p>200+ MW</p>
                </div>
                <div class="stat-card">
                    <h3>{recent}</h3>
                    <p>Last 30 Days</p>
                </div>
            </div>
            
            <div class="card">
                <h2>Data Sources</h2>
                <p><strong>Real-Time (Daily):</strong> 
                    <span class="status active">CAISO</span>
                    <span class="status active">NYISO</span>
                    <span class="status active">ISO-NE</span>
                    <span class="status active">SPP</span>
                </p>
                <p><strong>Berkeley Lab (Monthly):</strong> 
                    <span class="status active">MISO</span>
                    <span class="status active">ERCOT</span>
                    <span class="status active">PJM</span>
                </p>
                <p><strong>Requires Setup:</strong> ERCOT/PJM real-time (needs gridstatus/API)</p>
            </div>
            
            <div class="card">
                <h2>Top States by Project Count</h2>
                <table>
                    <tr><th>State</th><th>Projects</th><th>Total Capacity</th></tr>
                    {''.join([f"<tr><td>{s.state or 'Unknown'}</td><td>{s.count}</td><td>{s.total_mw:,.0f} MW</td></tr>" for s in top_states])}
                </table>
            </div>
            
            <div class="card">
                <h2>Quick Actions</h2>
                <a href="/run-monitor" class="btn btn-success">🔄 Run Scan</a>
                <a href="/projects" class="btn">📋 View Projects</a>
                <a href="/status" class="btn">🔧 Check Status</a>
                <a href="/export/csv" class="btn">📥 Export CSV</a>
            </div>
            
            <div class="card">
                <p><strong>Last scan:</strong> {last_run.run_date.strftime('%m/%d/%Y %H:%M') if last_run else 'Never'}</p>
            </div>
        </div>
    </body>
    </html>
    """
    return html

@app.route('/status')
def status():
    """System status page showing what's working"""
    monitor = HybridPowerMonitor()
    status_info = []
    
    # Test each source
    sources = {
        'CAISO': 'http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx',
        'NYISO': 'https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx',
        'ISO-NE': 'https://irtt.iso-ne.com/reports/external',
        'SPP': 'https://opsportal.spp.org/Studies/GenerateActiveCSV',
        'ERCOT': 'API (gridstatus)',
        'PJM': 'API Key Required'
    }
    
    for source, url in sources.items():
        status_info.append(f"<tr><td>{source}</td><td>{url}</td></tr>")
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>System Status</title>
        <style>
            body {{ font-family: Arial; margin: 40px; }}
            table {{ width: 100%; border-collapse: collapse; }}
            th, td {{ padding: 10px; border: 1px solid #ddd; text-align: left; }}
            .success {{ color: green; }}
            .error {{ color: red; }}
        </style>
    </head>
    <body>
        <h1>System Status</h1>
        <p><strong>gridstatus Library:</strong> {'✅ Installed' if GRIDSTATUS_AVAILABLE else '❌ Not Installed'}</p>
        <p><strong>PJM API Key:</strong> {'✅ Set' if os.getenv('PJM_API_KEY') else '❌ Not Set'}</p>
        
        <h2>Data Sources</h2>
        <table>
            <tr><th>Source</th><th>URL/Method</th></tr>
            {''.join(status_info)}
        </table>
        
        <h3>To Install gridstatus:</h3>
        <pre>pip install gridstatus</pre>
        
        <p><a href="/">Back to Dashboard</a></p>
    </body>
    </html>
    """

@app.route('/projects')
def projects():
    project_type = request.args.get('type')
    min_capacity = request.args.get('min_capacity', type=float)
    state = request.args.get('state')
    
    query = PowerProject.active()
    if project_type:
        query = query.filter_by(project_type=project_type)
    if min_capacity:
        query = query.filter(PowerProject.capacity_mw >= min_capacity)
    if state:
        query = query.filter_by(state=state)
    
    projects = query.order_by(PowerProject.capacity_mw.desc()).limit(500).all()
    
    rows = ""
    for p in projects:
        type_color = {
            'datacenter': '#4CAF50',
            'solar': '#FFC107',
            'wind': '#2196F3',
            'storage': '#9C27B0',
            'manufacturing': '#FF5722'
        }.get(p.project_type, '#9E9E9E')
        
        rows += f"""
        <tr>
            <td><strong>{p.project_name or 'Unnamed'}</strong><br>
                <small>ID: {p.request_id}</small></td>
            <td><strong>{p.capacity_mw:,.0f}</strong> MW</td>
            <td>{p.location or p.county or 'Unknown'}, {p.state or ''}</td>
            <td>{p.customer or p.developer or 'N/A'}</td>
            <td><span style="padding: 3px 8px; background: {type_color}; color: white; border-radius: 3px; font-size: 12px;">{p.project_type}</span></td>
            <td>{p.status or 'Active'}</td>
            <td>{p.source}</td>
            <td>{p.created_at.strftime('%m/%d')}</td>
        </tr>
        """
    
    if not projects:
        rows = "<tr><td colspan='8' style='text-align: center; padding: 40px;'>No projects found. Run monitoring to fetch data.</td></tr>"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Power Projects</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; background: #f0f2f5; }}
            .nav {{ background: #2c3e50; padding: 0; }}
            .nav a {{ color: white; padding: 15px 20px; display: inline-block; text-decoration: none; }}
            .nav a:hover {{ background: #34495e; }}
            .container {{ max-width: 1600px; margin: 0 auto; padding: 20px; }}
            .filters {{ background: white; padding: 15px; margin: 20px 0; border-radius: 5px; }}
            .filters a {{ margin-right: 10px; padding: 5px 10px; background: #007bff; color: white; text-decoration: none; border-radius: 3px; }}
            table {{ width: 100%; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            th {{ background: #34495e; color: white; padding: 12px; text-align: left; }}
            td {{ padding: 12px; border-bottom: 1px solid #ecf0f1; }}
            tr:hover {{ background: #f8f9fa; }}
        </style>
    </head>
    <body>
        <div class="nav">
            <a href="/">🏠 Dashboard</a>
            <a href="/projects">📊 All Projects</a>
            <a href="/projects?type=datacenter">🖥️ Data Centers</a>
            <a href="/projects?type=solar">☀️ Solar</a>
            <a href="/projects?type=wind">💨 Wind</a>
            <a href="/projects?min_capacity=200">⚡ 200+ MW</a>
        </div>
        
        <div class="container">
            <h1>Active Power Projects</h1>
            
            <div class="filters">
                <strong>Quick Filters:</strong>
                <a href="/projects?state=CA">California</a>
                <a href="/projects?state=TX">Texas</a>
                <a href="/projects?state=NY">New York</a>
                <a href="/projects?min_capacity=500">500+ MW</a>
            </div>
            
            <table>
                <thead>
                    <tr>
                        <th>Project Name / ID</th>
                        <th>Capacity</th>
                        <th>Location</th>
                        <th>Customer</th>
                        <th>Type</th>
                        <th>Status</th>
                        <th>Source</th>
                        <th>Added</th>
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
        </div>
    </body>
    </html>
    """
    return html

@app.route('/run-monitor')
def run_monitor():
    try:
        monitor = HybridPowerMonitor()
        result = monitor.run_comprehensive_monitoring()
        
        source_details = "<ul>"
        for source, count in result['by_source'].items():
            status = "✅" if count > 0 else "❌"
            source_details += f"<li>{status} {source}: {count} projects</li>"
        source_details += "</ul>"
        
        berkeley_lab_info = ""
        if 'berkeley_lab_sync' in result:
            bl_result = result['berkeley_lab_sync']
            berkeley_lab_info = f"""
            <div style="background: #e7f3ff; border-left: 4px solid #2196F3; padding: 15px; margin: 15px 0;">
                <h3>🏛️ Berkeley Lab Monthly Sync Ran!</h3>
                <p><strong>Projects Found:</strong> {bl_result.get('projects_found', 0)}</p>
                <p><strong>New Stored:</strong> {bl_result.get('projects_stored', 0)}</p>
                <p><strong>Next Sync:</strong> {bl_result.get('next_sync').strftime('%Y-%m-%d') if bl_result.get('next_sync') else 'N/A'}</p>
            </div>
            """
        
        return f"""
        <html>
        <body style="font-family: Arial; margin: 40px;">
            <h2>✅ Monitoring Complete</h2>
            <p><strong>gridstatus available:</strong> {'Yes' if result['gridstatus_available'] else 'No (using direct URLs)'}</p>
            <p><strong>Sources Checked:</strong> {result['sources_checked']}</p>
            <p><strong>Projects Found:</strong> {result['projects_found']}</p>
            <p><strong>New Stored:</strong> {result['projects_stored']}</p>
            <p><strong>Old Removed:</strong> {result.get('old_projects_removed', 0)}</p>
            <p><strong>Duration:</strong> {result['duration_seconds']}s</p>
            {source_details}
            {berkeley_lab_info}
            <a href="/projects" style="padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px;">View Projects</a>
            <a href="/" style="padding: 10px 20px; background: #6c757d; color: white; text-decoration: none; border-radius: 5px; margin-left: 10px;">Dashboard</a>
            <a href="/berkeley-lab-sync" style="padding: 10px 20px; background: #17a2b8; color: white; text-decoration: none; border-radius: 5px; margin-left: 10px;">Berkeley Lab Status</a>
        </body>
        </html>
        """
    except Exception as e:
        logger.error(f"Monitoring error: {e}")
        return f"<html><body><h2>Error</h2><p>{str(e)}</p><a href='/'>Back</a></body></html>", 500

@app.route('/monitoring')
def monitoring():
    runs = MonitoringRun.query.order_by(MonitoringRun.run_date.desc()).limit(50).all()
    
    rows = ""
    for run in runs:
        details = {}
        try:
            details = json.loads(run.details) if run.details else {}
        except:
            pass
        
        rows += f"""
        <tr>
            <td>{run.run_date.strftime('%Y-%m-%d %H:%M:%S')}</td>
            <td>{run.sources_checked}</td>
            <td>{run.projects_found}</td>
            <td>{run.projects_stored}</td>
            <td>{run.duration_seconds:.1f}s</td>
            <td>{run.status}</td>
            <td>{', '.join([f"{k}:{v}" for k,v in details.items()])}</td>
        </tr>
        """
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Monitoring History</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; background: #f0f2f5; }}
            .nav {{ background: #2c3e50; padding: 0; }}
            .nav a {{ color: white; padding: 15px 20px; display: inline-block; text-decoration: none; }}
            .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
            .card {{ background: white; padding: 20px; margin: 20px 0; border-radius: 8px; }}
            table {{ width: 100%; border-collapse: collapse; }}
            th {{ background: #f8f9fa; padding: 12px; text-align: left; }}
            td {{ padding: 10px; border-bottom: 1px solid #ecf0f1; font-size: 14px; }}
            .btn {{ padding: 12px 24px; background: #28a745; color: white; text-decoration: none; border-radius: 5px; display: inline-block; }}
        </style>
    </head>
    <body>
        <div class="nav">
            <a href="/">🏠 Dashboard</a>
            <a href="/projects">📊 Projects</a>
            <a href="/monitoring">📡 Monitoring</a>
            <a href="/berkeley-lab-sync">🏛️ Berkeley Lab</a>
        </div>
        
        <div class="container">
            <h1>Monitoring History</h1>
            <div class="card">
                <a href="/run-monitor" class="btn">🔄 Run Scan Now</a>
            </div>
            <div class="card">
                <table>
                    <thead>
                        <tr>
                            <th>Date/Time</th>
                            <th>Sources</th>
                            <th>Found</th>
                            <th>Stored</th>
                            <th>Duration</th>
                            <th>Status</th>
                            <th>Details</th>
                        </tr>
                    </thead>
                    <tbody>{rows if rows else '<tr><td colspan="7">No runs yet</td></tr>'}</tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """

@app.route('/berkeley-lab-sync')
def berkeley_lab_sync_page():
    """Berkeley Lab sync status and manual trigger"""
    syncs = BerkeleyLabSync.query.order_by(BerkeleyLabSync.sync_date.desc()).limit(20).all()
    
    last_sync = syncs[0] if syncs else None
    
    rows = ""
    for sync in syncs:
        status_emoji = "✅" if sync.status == 'completed' else "❌"
        rows += f"""
        <tr>
            <td>{sync.sync_date.strftime('%Y-%m-%d %H:%M')}</td>
            <td>{sync.projects_found}</td>
            <td>{sync.projects_stored}</td>
            <td>{sync.duration_seconds:.1f}s</td>
            <td>{status_emoji} {sync.status}</td>
            <td>{sync.next_sync_date.strftime('%Y-%m-%d') if sync.next_sync_date else 'N/A'}</td>
        </tr>
        """
    
    next_sync_info = "No sync scheduled"
    if last_sync and last_sync.next_sync_date:
        days_until = (last_sync.next_sync_date - datetime.utcnow()).days
        next_sync_info = f"{last_sync.next_sync_date.strftime('%Y-%m-%d')} ({days_until} days)"
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Berkeley Lab Sync</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; background: #f0f2f5; }}
            .nav {{ background: #2c3e50; padding: 0; }}
            .nav a {{ color: white; padding: 15px 20px; display: inline-block; text-decoration: none; }}
            .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
            .card {{ background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            table {{ width: 100%; border-collapse: collapse; }}
            th {{ background: #f8f9fa; padding: 12px; text-align: left; }}
            td {{ padding: 10px; border-bottom: 1px solid #ecf0f1; font-size: 14px; }}
            .btn {{ padding: 12px 24px; background: #28a745; color: white; text-decoration: none; border-radius: 5px; display: inline-block; margin: 5px; }}
            .info-box {{ background: #e7f3ff; border-left: 4px solid #2196F3; padding: 15px; margin: 15px 0; }}
        </style>
    </head>
    <body>
        <div class="nav">
            <a href="/">🏠 Dashboard</a>
            <a href="/projects">📊 Projects</a>
            <a href="/monitoring">📡 Monitoring</a>
            <a href="/berkeley-lab-sync">🏛️ Berkeley Lab</a>
        </div>
        
        <div class="container">
            <h1>Berkeley Lab Monthly Sync</h1>
            
            <div class="card">
                <h2>About Berkeley Lab Data</h2>
                <p>Lawrence Berkeley National Laboratory publishes comprehensive interconnection queue data monthly/quarterly. This provides data for ISOs we don't have real-time access to:</p>
                <ul>
                    <li><strong>MISO</strong> - Midwest (14 states)</li>
                    <li><strong>ERCOT</strong> - Texas</li>
                    <li><strong>PJM</strong> - Mid-Atlantic (13 states)</li>
                </ul>
                <p>The data is synced automatically once per month to supplement our real-time sources (CAISO, NYISO, ISO-NE, SPP).</p>
            </div>
            
            <div class="info-box">
                <strong>Last Sync:</strong> {last_sync.sync_date.strftime('%Y-%m-%d %H:%M') if last_sync else 'Never'}<br>
                <strong>Next Scheduled Sync:</strong> {next_sync_info}<br>
                <strong>Status:</strong> {last_sync.status if last_sync else 'N/A'}
            </div>
            
            <div class="card">
                <a href="/run-berkeley-lab-sync" class="btn">🔄 Run Sync Now</a>
                <p style="color: #666; margin-top: 10px;">Note: Manual sync will reset the monthly timer</p>
            </div>
            
            <div class="card">
                <h2>Sync History</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Date</th>
                            <th>Found</th>
                            <th>Stored</th>
                            <th>Duration</th>
                            <th>Status</th>
                            <th>Next Sync</th>
                        </tr>
                    </thead>
                    <tbody>{rows if rows else '<tr><td colspan="6">No syncs yet</td></tr>'}</tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """

@app.route('/run-berkeley-lab-sync')
def run_berkeley_lab_sync_manual():
    """Manually trigger Berkeley Lab sync"""
    try:
        monitor = HybridPowerMonitor()
        result = monitor.run_berkeley_lab_sync()
        
        return f"""
        <html>
        <body style="font-family: Arial; margin: 40px;">
            <h2>{'✅' if not result.get('error') else '❌'} Berkeley Lab Sync Complete</h2>
            <p><strong>Projects Found:</strong> {result.get('projects_found', 0)}</p>
            <p><strong>New Stored:</strong> {result.get('projects_stored', 0)}</p>
            <p><strong>Duration:</strong> {result.get('duration', 0)}s</p>
            <p><strong>Next Sync:</strong> {result.get('next_sync').strftime('%Y-%m-%d') if result.get('next_sync') else 'N/A'}</p>
            {f'<p style="color: red;"><strong>Error:</strong> {result.get("error")}</p>' if result.get('error') else ''}
            <a href="/berkeley-lab-sync" style="padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px;">Back to Sync Page</a>
            <a href="/" style="padding: 10px 20px; background: #6c757d; color: white; text-decoration: none; border-radius: 5px; margin-left: 10px;">Dashboard</a>
        </body>
        </html>
        """
    except Exception as e:
        logger.error(f"Manual Berkeley Lab sync error: {e}")
        return f"<html><body><h2>Error</h2><p>{str(e)}</p><a href='/berkeley-lab-sync'>Back</a></body></html>", 500

@app.route('/export/csv')
def export_csv():
    projects = PowerProject.active().order_by(PowerProject.capacity_mw.desc()).all()
    
    csv_data = "Request ID,Project Name,Capacity (MW),Location,State,Customer,Type,Status,Source,Date Added\n"
    
    for p in projects:
        csv_data += f'"{p.request_id}","{p.project_name or ""}",{p.capacity_mw},"{p.location or p.county or ""}","{p.state or ""}","{p.customer or ""}","{p.project_type or ""}","{p.status or ""}","{p.source}",{p.created_at.strftime("%Y-%m-%d")}\n'
    
    response = app.response_class(
        csv_data,
        mimetype='text/csv',
        headers={"Content-Disposition": f"attachment;filename=power_projects_{datetime.now().strftime('%Y%m%d')}.csv"}
    )
    return response

@app.route('/init')
def init_db():
    try:
        db.create_all()
        return "Database initialized! <a href='/'>Go to Dashboard</a>"
    except Exception as e:
        return f"Error: {str(e)}", 500

@app.route('/reset-db')
def reset_database():
    try:
        db.drop_all()
        db.create_all()
        return """
        <html>
        <body style="font-family: Arial; margin: 40px;">
            <h2>✅ Database Reset Complete</h2>
            <a href="/" style="padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px;">Dashboard</a>
            <a href="/run-monitor" style="padding: 10px 20px; background: #28a745; color: white; text-decoration: none; border-radius: 5px; margin-left: 10px;">Run Monitor</a>
        </body>
        </html>
        """
    except Exception as e:
        return f"Error: {str(e)}", 500

# Create tables on startup
with app.app_context():
    try:
        db.create_all()
        logger.info(f"Database ready. gridstatus available: {GRIDSTATUS_AVAILABLE}")
    except Exception as e:
        logger.error(f"Database error: {e}")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
