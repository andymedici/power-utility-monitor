# FINAL_WORKING_ultra_monitor.py - Handles real ISO file formats
"""
FINAL FIX - Handles:
1. Metadata rows at top of Excel files
2. Multiple header row attempts
3. Correct PJM URLs
4. Real-world file formats
"""

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
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps

logger = logging.getLogger(__name__)

def retry_with_backoff(max_retries=3, backoff_factor=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Failed after {max_retries} attempts: {e}")
                        raise
                    wait_time = backoff_factor ** attempt
                    logger.warning(f"Retry {attempt + 1}/{max_retries} after {wait_time}s")
                    time.sleep(wait_time)
        return wrapper
    return decorator


class UltraPowerMonitor:
    """Ultimate power monitoring - FINAL WORKING VERSION"""
    
    def __init__(self):
        self.min_capacity_mw = 100
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def extract_capacity(self, value):
        """Extract MW capacity from any format"""
        if pd.isna(value) or not value:
            return None
        
        text = str(value).replace(',', '').strip().upper()
        text = text.replace('MW', '').replace('MEGAWATT', '').replace('MEGAWATTS', '').strip()
        
        try:
            capacity = float(text)
            return capacity if capacity >= self.min_capacity_mw else None
        except ValueError:
            pass
        
        match = re.search(r'(\d+\.?\d*)', text)
        if match:
            try:
                capacity = float(match.group(1))
                return capacity if capacity >= self.min_capacity_mw else None
            except ValueError:
                pass
        
        return None
    
    def generate_hash(self, data):
        key = f"{data.get('project_name', '')}_{data.get('capacity_mw', 0)}_{data.get('state', '')}"
        return hashlib.md5(key.lower().encode()).hexdigest()
    
    def calculate_hunter_score(self, project_data):
        """Calculate 0-100 confidence score"""
        score = 0
        signals = []
        
        name = project_data.get('project_name', '').lower()
        customer = project_data.get('customer', '').lower()
        fuel = project_data.get('fuel_type', '').lower()
        county = project_data.get('county', '').lower()
        state = project_data.get('state', '').lower()
        
        combined = f"{name} {customer} {fuel}"
        location = f"{county} {state}"
        
        # DC keywords (40 pts)
        dc_keywords = ['data center', 'datacenter', 'hyperscale', 'cloud', 'colocation', 'colo']
        if any(kw in combined for kw in dc_keywords):
            score += 40
            signals.append("DC keyword")
        
        # Tech companies (25 pts)
        tech_cos = ['amazon', 'aws', 'microsoft', 'azure', 'google', 'meta', 'facebook', 
                    'digitalrealty', 'equinix', 'cyrusone', 'qts']
        if any(co in combined for co in tech_cos):
            score += 25
            signals.append("Tech company")
        
        # Capacity (15 pts)
        cap = project_data.get('capacity_mw', 0)
        if cap >= 500:
            score += 15
            signals.append(f"{cap}MW")
        elif cap >= 300:
            score += 10
        elif cap >= 200:
            score += 5
        
        # Load indicators (10 pts)
        if any(w in fuel for w in ['load', 'demand', 'behind-meter']):
            score += 10
            signals.append("Load-only")
        
        # Hotspots (20 pts)
        hotspots = {'loudoun': 20, 'ashburn': 20, 'fairfax': 18, 'santa clara': 17}
        for place, pts in hotspots.items():
            if place in location:
                score += pts
                signals.append(f"Hotspot: {place.title()}")
                break
        
        # Negatives
        if any(w in combined for w in ['solar', 'wind', 'battery']):
            score = max(0, score - 25)
            signals.append("Not DC")
        
        return {
            'hunter_score': min(100, max(0, score)),
            'hunter_notes': ' | '.join(signals[:3]) if signals else 'No signals'
        }
    
    @retry_with_backoff(max_retries=2)
    def fetch_url(self, url, **kwargs):
        return self.session.get(url, **kwargs)
    
    def read_excel_smart(self, content, source_name):
        """Try to read Excel with different skip rows"""
        for skip in [0, 1, 2, 3]:
            try:
                df = pd.read_excel(BytesIO(content), skiprows=skip, engine='openpyxl')
                
                # Check if we got real columns
                cols = [str(c) for c in df.columns]
                unnamed_count = sum(1 for c in cols if 'Unnamed' in c)
                
                if unnamed_count < len(cols) * 0.5:  # Less than 50% unnamed
                    logger.info(f"{source_name}: Loaded with skiprows={skip}, columns: {cols[:5]}")
                    return df
            except Exception as e:
                continue
        
        # If all failed, try without skipping
        df = pd.read_excel(BytesIO(content), engine='openpyxl')
        logger.warning(f"{source_name}: Using default read, columns: {list(df.columns)[:5]}")
        return df
    
    # ==================== CAISO ====================
    def fetch_caiso(self):
        """CAISO - Fixed to handle metadata rows"""
        projects = []
        url = 'http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx'
        
        try:
            logger.info(f"CAISO: Fetching {url}")
            response = self.fetch_url(url, timeout=30, verify=False)
            
            if response.status_code == 200:
                df = self.read_excel_smart(response.content, "CAISO")
                logger.info(f"CAISO: {len(df)} rows, columns: {list(df.columns)[:10]}")
                
                # Find MW columns
                mw_cols = [c for c in df.columns if any(x in str(c).upper() for x in ['MW', 'CAPACITY', 'OUTPUT'])]
                logger.info(f"CAISO: MW columns found: {mw_cols[:3]}")
                
                if not mw_cols:
                    logger.error(f"CAISO: No MW columns found! All columns: {list(df.columns)}")
                    return []
                
                for idx, row in df.iterrows():
                    capacity = None
                    for col in mw_cols:
                        cap = self.extract_capacity(row.get(col))
                        if cap:
                            capacity = cap
                            break
                    
                    if capacity:
                        data = {
                            'request_id': f"CAISO_{idx}",
                            'project_name': str(row.get('Project Name', row.get('Generating Facility', f'Project {idx}')))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': 'CA',
                            'customer': str(row.get('Interconnection Customer', row.get('Developer', '')))[:500],
                            'utility': 'CAISO',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Fuel', row.get('Type', ''))),
                            'source': 'CAISO',
                            'source_url': url,
                        }
                        
                        score_result = self.calculate_hunter_score(data)
                        data.update(score_result)
                        data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                
                logger.info(f"CAISO: Found {len(projects)} projects >= {self.min_capacity_mw}MW")
                        
        except Exception as e:
            logger.error(f"CAISO error: {e}", exc_info=True)
        
        return projects
    
    # ==================== NYISO ====================
    def fetch_nyiso(self):
        """NYISO - Fixed"""
        projects = []
        url = 'https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx'
        
        try:
            logger.info(f"NYISO: Fetching {url}")
            response = self.fetch_url(url, timeout=30, verify=False)
            
            if response.status_code == 200:
                df = self.read_excel_smart(response.content, "NYISO")
                logger.info(f"NYISO: {len(df)} rows, columns: {list(df.columns)[:10]}")
                
                mw_cols = [c for c in df.columns if any(x in str(c).upper() for x in ['MW', 'CAPACITY', 'CAP'])]
                logger.info(f"NYISO: MW columns: {mw_cols[:3]}")
                
                if not mw_cols:
                    return []
                
                for idx, row in df.iterrows():
                    capacity = None
                    for col in mw_cols:
                        cap = self.extract_capacity(row.get(col))
                        if cap:
                            capacity = cap
                            break
                    
                    if capacity:
                        data = {
                            'request_id': f"NYISO_{idx}",
                            'project_name': str(row.get('Project Name', f'Project {idx}'))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': 'NY',
                            'customer': str(row.get('Developer', row.get('Customer', '')))[:500],
                            'utility': 'NYISO',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Type', row.get('Fuel', ''))),
                            'source': 'NYISO',
                            'source_url': url,
                        }
                        
                        score_result = self.calculate_hunter_score(data)
                        data.update(score_result)
                        data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                
                logger.info(f"NYISO: Found {len(projects)} projects")
                        
        except Exception as e:
            logger.error(f"NYISO error: {e}", exc_info=True)
        
        return projects
    
    # ==================== SPP ====================
    def fetch_spp(self):
        """SPP - Fixed CSV parsing"""
        projects = []
        url = 'https://opsportal.spp.org/Studies/GenerateActiveCSV'
        
        try:
            logger.info(f"SPP: Fetching {url}")
            response = self.fetch_url(url, timeout=30, verify=False)
            
            if response.status_code == 200:
                # Try different skiprows for CSV
                for skip in [0, 1, 2]:
                    try:
                        df = pd.read_csv(StringIO(response.text), skiprows=skip)
                        cols = [str(c) for c in df.columns]
                        
                        # Check if we got real data
                        if len(cols) > 5 and not all('Unnamed' in c for c in cols):
                            logger.info(f"SPP: Loaded with skiprows={skip}, columns: {cols[:5]}")
                            break
                    except:
                        continue
                
                logger.info(f"SPP: {len(df)} rows, columns: {list(df.columns)[:10]}")
                
                mw_cols = [c for c in df.columns if any(x in str(c).upper() for x in ['MW', 'SIZE', 'CAPACITY'])]
                logger.info(f"SPP: MW columns: {mw_cols[:3]}")
                
                if not mw_cols:
                    return []
                
                for idx, row in df.iterrows():
                    capacity = None
                    for col in mw_cols:
                        cap = self.extract_capacity(row.get(col))
                        if cap:
                            capacity = cap
                            break
                    
                    if capacity:
                        data = {
                            'request_id': f"SPP_{idx}",
                            'project_name': str(row.get('Project Name', f'Project {idx}'))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': str(row.get('State', ''))[:2],
                            'customer': str(row.get('Customer', ''))[:500],
                            'utility': 'SPP',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Fuel Type', '')),
                            'source': 'SPP',
                            'source_url': url,
                        }
                        
                        score_result = self.calculate_hunter_score(data)
                        data.update(score_result)
                        data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                
                logger.info(f"SPP: Found {len(projects)} projects")
                        
        except Exception as e:
            logger.error(f"SPP error: {e}", exc_info=True)
        
        return projects
    
    # ==================== PJM ====================
    def fetch_pjm(self):
        """PJM - Use actual working URL"""
        projects = []
        
        # PJM's actual public queue location
        url = 'https://services.pjm.com/PJMPlanningApi/api/Queue/ExportToXls'
        
        try:
            logger.info(f"PJM: Fetching {url}")
            response = self.fetch_url(url, timeout=30)
            
            if response.status_code == 200 and len(response.content) > 1000:
                df = self.read_excel_smart(response.content, "PJM")
                logger.info(f"PJM: {len(df)} rows, columns: {list(df.columns)[:10]}")
                
                mw_cols = [c for c in df.columns if any(x in str(c).upper() for x in ['MW', 'CAPACITY'])]
                logger.info(f"PJM: MW columns: {mw_cols[:3]}")
                
                if not mw_cols:
                    logger.warning("PJM: No MW columns, skipping")
                    return []
                
                for idx, row in df.iterrows():
                    capacity = None
                    for col in mw_cols:
                        cap = self.extract_capacity(row.get(col))
                        if cap:
                            capacity = cap
                            break
                    
                    if capacity:
                        data = {
                            'request_id': f"PJM_{idx}",
                            'project_name': str(row.get('Project Name', f'Project {idx}'))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': str(row.get('State', ''))[:2],
                            'customer': str(row.get('Customer', ''))[:500],
                            'utility': 'PJM',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Fuel Type', '')),
                            'source': 'PJM',
                            'source_url': url,
                        }
                        
                        score_result = self.calculate_hunter_score(data)
                        data.update(score_result)
                        data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                
                logger.info(f"PJM: Found {len(projects)} projects")
                        
        except Exception as e:
            logger.error(f"PJM error: {e}", exc_info=True)
        
        return projects
    
    # Simplified stubs for now
    def fetch_miso(self):
        logger.info("MISO: Disabled in this version")
        return []
    
    def fetch_isone(self):
        logger.info("ISO-NE: Disabled in this version")
        return []
    
    def fetch_ercot(self):
        logger.info("ERCOT: Disabled in this version")
        return []
    
    def fetch_ferc_elibrary(self):
        logger.info("FERC: Disabled in this version")
        return []
    
    def fetch_virginia_scc(self):
        logger.info("VA SCC: Disabled in this version")
        return []
    
    def fetch_utility_news(self):
        logger.info("Utility News: Disabled in this version")
        return []
    
    # ==================== MAIN RUNNER ====================
    def run_ultra_monitoring(self, max_workers=4):
        """Run with working sources only"""
        start_time = time.time()
        all_projects = []
        source_stats = {}
        
        monitors = [
            ('CAISO', self.fetch_caiso),
            ('NYISO', self.fetch_nyiso),
            ('PJM', self.fetch_pjm),
            ('SPP', self.fetch_spp),
        ]
        
        logger.info(f"FINAL: Running {len(monitors)} sources...")
        
        for source_name, fetch_func in monitors:
            try:
                logger.info(f"→ Fetching {source_name}...")
                source_projects = fetch_func()
                all_projects.extend(source_projects)
                source_stats[source_name] = len(source_projects)
                logger.info(f"✓ {source_name}: {len(source_projects)} projects")
            except Exception as e:
                logger.error(f"✗ {source_name} failed: {e}")
                source_stats[source_name] = 0
        
        duration = time.time() - start_time
        
        high_conf = sum(1 for p in all_projects if p.get('hunter_score', 0) >= 70)
        med_conf = sum(1 for p in all_projects if 40 <= p.get('hunter_score', 0) < 70)
        
        logger.info(f"SCAN COMPLETE: {len(all_projects)} projects, {high_conf} high-confidence DCs")
        
        return {
            'sources_checked': len(monitors),
            'projects_found': len(all_projects),
            'duration_seconds': round(duration, 2),
            'by_source': source_stats,
            'all_projects': all_projects,
            'statistics': {
                'high_confidence_dc': high_conf,
                'medium_confidence_dc': med_conf,
                'total_capacity_mw': sum(p.get('capacity_mw', 0) for p in all_projects)
            }
        }
