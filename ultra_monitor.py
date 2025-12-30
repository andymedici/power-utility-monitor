# ultra_monitor.py - Complete Ultra Power Monitor with 6 ISOs
"""
COMPLETE VERSION with 6 ISOs:
- CAISO (California): 262 projects ✅
- NYISO (New York): 83 projects ✅
- SPP (9 states): 758 projects ✅
- MISO (14 states): 600-1000 projects NEW!
- ISO-NE (6 states): 200-400 projects NEW!
- ERCOT (Texas): 150-300 projects NEW!

Total Expected: 2,053-2,803 projects
Coverage: 40+ states
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
    """Complete power monitoring with 6 ISOs covering 40+ states"""
    
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
        """Calculate 0-100 confidence score for data center detection"""
        score = 0
        signals = []
        
        name = project_data.get('project_name', '').lower()
        customer = project_data.get('customer', '').lower()
        fuel = project_data.get('fuel_type', '').lower()
        county = project_data.get('county', '').lower()
        state = project_data.get('state', '').lower()
        
        combined = f"{name} {customer} {fuel}"
        location = f"{county} {state}"
        
        # 1. DC keywords (40 pts)
        dc_keywords = ['data center', 'datacenter', 'data centre', 'hyperscale', 'cloud', 
                       'colocation', 'colo', 'server farm', 'computing facility', 'edge computing']
        if any(kw in combined for kw in dc_keywords):
            score += 40
            signals.append("DC keyword")
        
        # 2. Tech companies (25 pts)
        tech_cos = ['amazon', 'aws', 'amazon web services', 'microsoft', 'azure', 
                    'google', 'gcp', 'alphabet', 'meta', 'facebook', 'apple', 'oracle',
                    'digitalrealty', 'digital realty', 'equinix', 'cyrusone', 'qts', 
                    'coresite', 'iron mountain', 'switch', 'vantage', 'aligned']
        if any(co in combined for co in tech_cos):
            score += 25
            signals.append("Tech company")
        
        # 3. Capacity (15 pts)
        cap = project_data.get('capacity_mw', 0)
        if cap >= 500:
            score += 15
            signals.append(f"{cap}MW")
        elif cap >= 300:
            score += 10
        elif cap >= 200:
            score += 5
        
        # 4. Load indicators (10 pts)
        if any(w in fuel for w in ['load', 'demand', 'behind-meter', 'customer load', 'offtake']):
            score += 10
            signals.append("Load-only")
        
        # 5. Geographic hotspots (20 pts)
        hotspots = {
            'loudoun': 20, 'ashburn': 20, 'leesburg': 18, 'fairfax': 18, 
            'prince william': 17, 'santa clara': 17, 'san jose': 16,
            'king': 16, 'seattle': 15, 'quincy': 18, 'hillsboro': 17,
            'dallas': 15, 'richardson': 15, 'chicago': 14, 'cook': 14,
            'phoenix': 14, 'maricopa': 14, 'chandler': 14, 'atlanta': 13,
            'fulton': 13, 'columbus': 13, 'franklin': 13, 'new albany': 15
        }
        for place, pts in hotspots.items():
            if place in location:
                score += pts
                signals.append(f"Hotspot: {place.title()}")
                break
        
        # 6. Suspicious naming (10 pts)
        suspicious = [
            (r'project [a-z]?\d+', 'Generic naming'),
            (r'facility [a-z]', 'Facility code'),
            (r'campus [a-z]?\d*', 'Campus naming'),
            (r'confidential', 'Confidential'),
        ]
        for pattern, label in suspicious:
            if re.search(pattern, combined):
                score += 5
                signals.append(label)
                break
        
        # 7. Negative signals
        if any(w in combined for w in ['solar', 'wind', 'battery', 'photovoltaic']):
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
        """Try to read Excel with different skip rows to handle metadata"""
        for skip in [0, 1, 2, 3]:
            try:
                df = pd.read_excel(BytesIO(content), skiprows=skip, engine='openpyxl')
                
                # Check if we got real columns
                cols = [str(c) for c in df.columns]
                unnamed_count = sum(1 for c in cols if 'Unnamed' in c)
                
                if unnamed_count < len(cols) * 0.5:  # Less than 50% unnamed
                    logger.info(f"{source_name}: Using skiprows={skip}, columns: {cols[:5]}")
                    return df
            except Exception as e:
                continue
        
        # Fallback: use default
        df = pd.read_excel(BytesIO(content), engine='openpyxl')
        logger.warning(f"{source_name}: Using default, columns: {list(df.columns)[:5]}")
        return df
    
    # ==================== CAISO ====================
    def fetch_caiso(self):
        """CAISO - California"""
        projects = []
        url = 'http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx'
        
        try:
            logger.info(f"CAISO: Fetching {url}")
            response = self.fetch_url(url, timeout=30, verify=False)
            
            if response.status_code == 200:
                df = self.read_excel_smart(response.content, "CAISO")
                logger.info(f"CAISO: {len(df)} rows")
                
                mw_cols = [c for c in df.columns if any(x in str(c).upper() for x in ['MW', 'CAPACITY', 'OUTPUT'])]
                
                if not mw_cols:
                    logger.error(f"CAISO: No MW columns found!")
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
                            'project_name': str(row.get('Project Name', f'Project {idx}'))[:500],
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
                
                logger.info(f"CAISO: Found {len(projects)} projects")
                        
        except Exception as e:
            logger.error(f"CAISO error: {e}", exc_info=True)
        
        return projects
    
    # ==================== NYISO ====================
    def fetch_nyiso(self):
        """NYISO - New York"""
        projects = []
        url = 'https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx'
        
        try:
            logger.info(f"NYISO: Fetching {url}")
            response = self.fetch_url(url, timeout=30, verify=False)
            
            if response.status_code == 200:
                df = self.read_excel_smart(response.content, "NYISO")
                logger.info(f"NYISO: {len(df)} rows")
                
                mw_cols = [c for c in df.columns if any(x in str(c).upper() for x in ['MW', 'CAPACITY', 'CAP'])]
                
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
        """SPP - 9 states"""
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
                        
                        if len(cols) > 5 and not all('Unnamed' in c for c in cols):
                            logger.info(f"SPP: Loaded with skiprows={skip}")
                            break
                    except:
                        continue
                
                logger.info(f"SPP: {len(df)} rows")
                
                mw_cols = [c for c in df.columns if any(x in str(c).upper() for x in ['MW', 'SIZE', 'CAPACITY'])]
                
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
    
    # ==================== MISO (NEW!) ====================
    def fetch_miso(self):
        """MISO - 14 Midwest states"""
        projects = []
        
        # Try multiple MISO data sources
        urls = [
            'https://cdn.misoenergy.org/GIQ%20PUBLIC%20DATA625353.xlsx',
            'https://cdn.misoenergy.org/GIQ%20PUBLIC%20DATA625353.xls',
        ]
        
        try:
            logger.info("MISO: Attempting to fetch queue data...")
            
            for url in urls:
                try:
                    logger.info(f"MISO: Trying {url}")
                    response = self.fetch_url(url, timeout=60)
                    
                    if response.status_code == 200 and len(response.content) > 10000:
                        df = self.read_excel_smart(response.content, "MISO")
                        
                        if len(df) < 50:
                            logger.info(f"MISO: File too small ({len(df)} rows), trying next URL")
                            continue
                        
                        logger.info(f"MISO: SUCCESS! {len(df)} rows")
                        
                        # Find MW columns
                        mw_cols = [c for c in df.columns if any(x in str(c).upper() for x in ['MW', 'CAPACITY', 'SIZE', 'NAMEPLATE'])]
                        logger.info(f"MISO: MW columns: {mw_cols[:3]}")
                        
                        if not mw_cols:
                            logger.warning("MISO: No MW columns found")
                            continue
                        
                        # Process projects
                        for idx, row in df.iterrows():
                            capacity = None
                            for col in mw_cols:
                                cap = self.extract_capacity(row.get(col))
                                if cap:
                                    capacity = cap
                                    break
                            
                            if capacity:
                                data = {
                                    'request_id': f"MISO_{row.get('Queue Number', row.get('J-Number', idx))}",
                                    'project_name': str(row.get('Project Name', row.get('Resource Name', f'Project {idx}')))[:500],
                                    'capacity_mw': capacity,
                                    'county': str(row.get('County', ''))[:200],
                                    'state': str(row.get('State', ''))[:2],
                                    'customer': str(row.get('Interconnection Customer', row.get('Developer', '')))[:500],
                                    'utility': 'MISO',
                                    'status': str(row.get('Status', 'Active')),
                                    'fuel_type': str(row.get('Fuel Type', row.get('Technology', ''))),
                                    'source': 'MISO',
                                    'source_url': url,
                                }
                                
                                score_result = self.calculate_hunter_score(data)
                                data.update(score_result)
                                data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                                data['data_hash'] = self.generate_hash(data)
                                projects.append(data)
                        
                        logger.info(f"MISO: Found {len(projects)} projects")
                        break  # Success!
                        
                except Exception as e:
                    logger.warning(f"MISO: Failed URL {url}: {e}")
                    continue
            
            if not projects:
                logger.warning("MISO: No valid data found. Check MISO website manually.")
                        
        except Exception as e:
            logger.error(f"MISO error: {e}", exc_info=True)
        
        return projects
    
    # ==================== ISO-NE (NEW!) ====================
    def fetch_isone(self):
        """ISO-NE - 6 New England states"""
        projects = []
        
        # ISO-NE has IRTT (Interconnection Request Tracking Tool)
        urls = [
            'https://irtt.iso-ne.com/reports/external/export?format=xlsx',
            'https://www.iso-ne.com/static-assets/documents/interconnection_queue.xlsx',
        ]
        
        try:
            logger.info("ISO-NE: Attempting to fetch queue data...")
            
            for url in urls:
                try:
                    logger.info(f"ISO-NE: Trying {url}")
                    response = self.fetch_url(url, timeout=60)
                    
                    if response.status_code == 200 and len(response.content) > 5000:
                        df = self.read_excel_smart(response.content, "ISO-NE")
                        
                        if len(df) < 10:
                            logger.info(f"ISO-NE: File too small ({len(df)} rows), trying next URL")
                            continue
                        
                        logger.info(f"ISO-NE: SUCCESS! {len(df)} rows")
                        
                        # Find MW columns
                        mw_cols = [c for c in df.columns if any(x in str(c).upper() for x in ['MW', 'NET MW', 'CAPACITY'])]
                        logger.info(f"ISO-NE: MW columns: {mw_cols[:3]}")
                        
                        if not mw_cols:
                            logger.warning("ISO-NE: No MW columns found")
                            continue
                        
                        # Process projects
                        for idx, row in df.iterrows():
                            capacity = None
                            for col in mw_cols:
                                cap = self.extract_capacity(row.get(col))
                                if cap:
                                    capacity = cap
                                    break
                            
                            if capacity:
                                # Map state abbreviations
                                state = str(row.get('State', row.get('ST', '')))[:2]
                                
                                data = {
                                    'request_id': f"ISONE_{row.get('Queue Position', row.get('QP', idx))}",
                                    'project_name': str(row.get('Project Name', f'Project {idx}'))[:500],
                                    'capacity_mw': capacity,
                                    'county': str(row.get('County', ''))[:200],
                                    'state': state,
                                    'customer': str(row.get('Interconnection Customer', row.get('Developer', '')))[:500],
                                    'utility': 'ISO-NE',
                                    'status': str(row.get('Status', 'Active')),
                                    'fuel_type': str(row.get('Fuel Type', row.get('Unit', ''))),
                                    'source': 'ISO-NE',
                                    'source_url': url,
                                }
                                
                                score_result = self.calculate_hunter_score(data)
                                data.update(score_result)
                                data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                                data['data_hash'] = self.generate_hash(data)
                                projects.append(data)
                        
                        logger.info(f"ISO-NE: Found {len(projects)} projects")
                        break  # Success!
                        
                except Exception as e:
                    logger.warning(f"ISO-NE: Failed URL {url}: {e}")
                    continue
            
            if not projects:
                logger.warning("ISO-NE: No valid data found. Check ISO-NE website manually.")
                        
        except Exception as e:
            logger.error(f"ISO-NE error: {e}", exc_info=True)
        
        return projects
    
    # ==================== ERCOT (NEW!) ====================
    def fetch_ercot(self):
        """ERCOT - Texas"""
        projects = []
        
        # ERCOT publishes GIS reports monthly
        # Try to get the latest one
        try:
            logger.info("ERCOT: Attempting to fetch GIS report...")
            
            # Try current and previous months
            today = datetime.now()
            months_to_try = []
            for i in range(6):  # Try last 6 months
                month_date = today - timedelta(days=30*i)
                months_to_try.append(month_date)
            
            for month_date in months_to_try:
                # ERCOT format: GIS_Report_YYYYMM.xlsx
                month_str = month_date.strftime('%Y%m')
                urls = [
                    f'https://www.ercot.com/files/docs/{month_date.year}/{month_date.month:02d}/GIS_Report_{month_str}.xlsx',
                    f'https://mis.ercot.com/misapp/GetReports.do?reportTypeId=15933&reportTitle=GIS%20Report&showHTMLView=&mimicKey',
                ]
                
                for url in urls:
                    try:
                        logger.info(f"ERCOT: Trying {month_str}")
                        response = self.fetch_url(url, timeout=60)
                        
                        if response.status_code == 200 and len(response.content) > 10000:
                            df = self.read_excel_smart(response.content, "ERCOT")
                            
                            if len(df) < 20:
                                continue
                            
                            logger.info(f"ERCOT: SUCCESS! {len(df)} rows from {month_str}")
                            
                            # Find MW columns
                            mw_cols = [c for c in df.columns if any(x in str(c).upper() for x in ['MW', 'CAPACITY', 'SIZE', 'NAMEPLATE'])]
                            logger.info(f"ERCOT: MW columns: {mw_cols[:3]}")
                            
                            if not mw_cols:
                                continue
                            
                            # Process projects
                            for idx, row in df.iterrows():
                                capacity = None
                                for col in mw_cols:
                                    cap = self.extract_capacity(row.get(col))
                                    if cap:
                                        capacity = cap
                                        break
                                
                                if capacity:
                                    data = {
                                        'request_id': f"ERCOT_{row.get('Project Code', row.get('INR', idx))}",
                                        'project_name': str(row.get('Project Name', f'Project {idx}'))[:500],
                                        'capacity_mw': capacity,
                                        'county': str(row.get('County', ''))[:200],
                                        'state': 'TX',
                                        'customer': str(row.get('Interconnecting Entity', row.get('Developer', '')))[:500],
                                        'utility': 'ERCOT',
                                        'status': str(row.get('Status', 'Active')),
                                        'fuel_type': str(row.get('Fuel', row.get('Technology', ''))),
                                        'source': 'ERCOT',
                                        'source_url': url,
                                    }
                                    
                                    score_result = self.calculate_hunter_score(data)
                                    data.update(score_result)
                                    data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                                    data['data_hash'] = self.generate_hash(data)
                                    projects.append(data)
                            
                            logger.info(f"ERCOT: Found {len(projects)} projects")
                            return projects  # Success!
                            
                    except Exception as e:
                        logger.debug(f"ERCOT: Failed {month_str}: {e}")
                        continue
            
            logger.warning("ERCOT: No valid data found. Check ERCOT website manually.")
                        
        except Exception as e:
            logger.error(f"ERCOT error: {e}", exc_info=True)
        
        return projects
    
    # ==================== MAIN RUNNER ====================
    def run_ultra_monitoring(self, max_workers=4):
        """Run monitoring with all 6 ISOs"""
        start_time = time.time()
        all_projects = []
        source_stats = {}
        
        # All 6 ISOs enabled!
        monitors = [
            ('CAISO', self.fetch_caiso),
            ('NYISO', self.fetch_nyiso),
            ('SPP', self.fetch_spp),
            ('MISO', self.fetch_miso),        # NEW!
            ('ISO-NE', self.fetch_isone),     # NEW!
            ('ERCOT', self.fetch_ercot),      # NEW!
        ]
        
        logger.info(f"COMPLETE SCAN: Running {len(monitors)} ISOs (6 sources)...")
        
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
