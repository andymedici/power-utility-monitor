# enhanced_monitor.py - Drop-in replacement with improved coverage
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
    """Decorator for retry logic with exponential backoff"""
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
                    logger.warning(f"Retry {attempt + 1}/{max_retries} after {wait_time}s: {e}")
                    time.sleep(wait_time)
            
        return wrapper
    return decorator


class EnhancedPowerMonitor:
    """Enhanced power monitoring with better coverage and detection"""
    
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
        
        # Try direct conversion
        try:
            capacity = float(text)
            if capacity >= self.min_capacity_mw:
                return capacity
        except:
            pass
        
        # Try extracting number from text
        match = re.search(r'(\d+\.?\d*)', text)
        if match:
            try:
                capacity = float(match.group(1))
                if capacity >= self.min_capacity_mw:
                    return capacity
            except:
                pass
        
        return None
    
    def generate_hash(self, data):
        """Generate unique hash for duplicate detection"""
        key = f"{data.get('project_name', '')}_{data.get('capacity_mw', 0)}_{data.get('location', '')}_{data.get('source', '')}"
        return hashlib.md5(key.lower().encode()).hexdigest()
    
    def calculate_hunter_score(self, project_data):
        """
        Advanced data center detection scoring (0-100)
        Returns: dict with score, notes, and all signals
        """
        score = 0
        signals = []
        
        # Combine all text fields
        name = project_data.get('project_name', '').lower()
        customer = project_data.get('customer', '').lower()
        fuel = project_data.get('fuel_type', '').lower()
        county = project_data.get('county', '').lower()
        state = project_data.get('state', '').lower()
        
        combined_text = f"{name} {customer} {fuel}"
        location = f"{county} {state}"
        
        # 1. EXPLICIT DATA CENTER KEYWORDS (40 points)
        dc_keywords = [
            'data center', 'datacenter', 'data centre',
            'hyperscale', 'cloud', 'colocation', 'colo',
            'server farm', 'computing facility',
            'edge computing', 'edge data', 'compute',
            'bit barn', 'server hub'
        ]
        
        for keyword in dc_keywords:
            if keyword in combined_text:
                score += 40
                signals.append(f"DC keyword: '{keyword}'")
                break
        
        # 2. TECH COMPANY INDICATORS (25 points)
        tech_companies = [
            'amazon', 'aws', 'microsoft', 'azure', 'google', 'gcp', 'alphabet',
            'meta', 'facebook', 'apple', 'oracle', 'ibm', 'salesforce',
            'digitalrealty', 'digital realty', 'equinix', 'cyrusone', 'qts',
            'iron mountain', 'switch', 'coresite', 'vantage', 'vantage data',
            'aligned', 'flexential', 'cloudflare', 'akamai',
            'quantum loophole', 'ql', 'stream data', 'compass datacenters',
            'edged energy', 'scale datacenter', 'prime datacenter'
        ]
        
        for company in tech_companies:
            if company in combined_text:
                score += 25
                signals.append(f"Tech company: {company.title()}")
                break
        
        # 3. CAPACITY SIGNALS (15 points max)
        capacity = project_data.get('capacity_mw', 0)
        
        if capacity >= 500:
            score += 15
            signals.append(f"Very high load: {capacity}MW")
        elif capacity >= 300:
            score += 12
            signals.append(f"High load: {capacity}MW")
        elif capacity >= 200:
            score += 8
            signals.append(f"Notable load: {capacity}MW")
        elif capacity >= 150:
            score += 5
            signals.append(f"Elevated load: {capacity}MW")
        
        # 4. FUEL TYPE / LOAD SIGNALS (10 points)
        load_indicators = [
            'load', 'demand', 'behind-meter', 'behind meter',
            'customer load', 'behind the meter', 'btm'
        ]
        
        for indicator in load_indicators:
            if indicator in fuel.lower():
                score += 10
                signals.append("Load-only project")
                break
        
        # 5. GEOGRAPHIC HOTSPOTS (20 points max)
        dc_hotspots = {
            # Virginia - THE largest DC market globally
            'loudoun': 20, 'ashburn': 20, 'leesburg': 18, 
            'fairfax': 18, 'prince william': 17, 'alexandria': 15,
            'manassas': 16, 'sterling': 18, 'culpeper': 15,
            
            # Silicon Valley
            'santa clara': 17, 'san jose': 16, 'sunnyvale': 15,
            'mountain view': 15, 'palo alto': 14,
            
            # Seattle/Oregon
            'king county': 16, 'seattle': 15, 'quincy': 18,
            'hillsboro': 17, 'portland': 14, 'the dalles': 16,
            'prineville': 17, 'morrow': 16,
            
            # Texas
            'dallas': 15, 'richardson': 15, 'fort worth': 14,
            'plano': 14, 'san antonio': 13, 'austin': 14,
            
            # Chicago
            'chicago': 14, 'cook county': 14, 'elk grove': 13,
            
            # Phoenix
            'phoenix': 14, 'maricopa': 14, 'chandler': 14, 'mesa': 13,
            
            # Atlanta
            'atlanta': 13, 'fulton': 13, 'gwinnett': 12,
            
            # Ohio
            'columbus': 13, 'franklin': 13, 'dublin': 12, 'new albany': 15,
            
            # North Carolina
            'raleigh': 12, 'durham': 12, 'wake': 12, 'charlotte': 11
        }
        
        location_lower = location.lower()
        for place, points in dc_hotspots.items():
            if place in location_lower:
                score += points
                signals.append(f"DC hotspot: {place.title()}")
                break
        
        # 6. SUSPICIOUS NAMING PATTERNS (10 points)
        suspicious_patterns = [
            (r'project [a-z]?\d+', 'Generic naming'),
            (r'facility [a-z]', 'Facility code'),
            (r'campus [a-z]?\d*', 'Campus naming'),
            (r'site [a-z\d]+', 'Site code'),
            (r'\bllc\b.*\bllc\b', 'Multiple LLCs'),
            (r'holdings? (?:llc|inc)', 'Holdings entity'),
            (r'development \d+', 'Dev project'),
            (r'ventures? (?:llc|inc)', 'Ventures entity'),
            (r'confidential', 'Confidential project'),
            (r'tbd\b|to be determined', 'TBD naming')
        ]
        
        for pattern, label in suspicious_patterns:
            if re.search(pattern, combined_text):
                score += 5
                signals.append(label)
                break
        
        # 7. NEGATIVE SIGNALS (reduce score significantly)
        negative_keywords = [
            ('solar', 25), ('wind', 25), ('battery', 20), ('storage', 20),
            ('photovoltaic', 25), ('bess', 20), ('renewable', 15),
            ('biomass', 20), ('landfill', 20), ('waste', 15),
            ('natural gas', 20), ('combined cycle', 20), ('gas turbine', 20),
            ('coal', 25), ('nuclear', 25), ('hydro', 20)
        ]
        
        for keyword, penalty in negative_keywords:
            if keyword in combined_text:
                score = max(0, score - penalty)
                signals.append(f"Not DC: {keyword}")
                break
        
        # Cap score at 100
        score = min(100, max(0, score))
        
        return {
            'hunter_score': score,
            'hunter_notes': ' | '.join(signals[:3]) if signals else 'No strong signals',
            'all_signals': signals
        }
    
    @retry_with_backoff(max_retries=2)
    def fetch_url(self, url, **kwargs):
        """Fetch URL with retry logic"""
        return self.session.get(url, **kwargs)
    
    # ==================== CAISO ====================
    def fetch_caiso(self):
        """CAISO - California"""
        projects = []
        excel_url = 'http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx'
        
        try:
            logger.info(f"CAISO: Fetching from {excel_url}")
            response = self.fetch_url(excel_url, timeout=30, verify=False)
            
            if response.status_code == 200:
                df = pd.read_excel(BytesIO(response.content))
                logger.info(f"CAISO: Processing {len(df)} rows")
                
                for _, row in df.iterrows():
                    # Try multiple capacity columns
                    capacity = None
                    for col in ['MW', 'Capacity (MW)', 'Max Output (MW)', 'Capacity']:
                        if col in df.columns:
                            capacity = self.extract_capacity(row.get(col))
                            if capacity:
                                break
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        data = {
                            'request_id': f"CAISO_{row.get('Queue Number', row.get('Queue ID', 'UNK'))}",
                            'project_name': str(row.get('Project Name', row.get('Generating Facility', 'Unknown')))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': 'CA',
                            'customer': str(row.get('Interconnection Customer', row.get('Developer', '')))[:500],
                            'utility': 'CAISO',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Fuel', row.get('Type', ''))),
                            'source': 'CAISO',
                            'source_url': excel_url,
                        }
                        
                        # Calculate hunter score
                        score_result = self.calculate_hunter_score(data)
                        data['hunter_score'] = score_result['hunter_score']
                        data['hunter_notes'] = score_result['hunter_notes']
                        data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"CAISO error: {e}")
        
        logger.info(f"CAISO: Found {len(projects)} projects (>= {self.min_capacity_mw}MW)")
        return projects
    
    # ==================== NYISO ====================
    def fetch_nyiso(self):
        """NYISO - New York"""
        projects = []
        excel_url = 'https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx'
        
        try:
            logger.info(f"NYISO: Fetching from {excel_url}")
            response = self.fetch_url(excel_url, timeout=30, verify=False)
            
            if response.status_code == 200:
                df = pd.read_excel(BytesIO(response.content))
                logger.info(f"NYISO: Processing {len(df)} rows")
                
                for _, row in df.iterrows():
                    capacity = None
                    for col in ['MW', 'Capacity (MW)', 'Summer Cap', 'Winter Cap', 'Capacity']:
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
                            'customer': str(row.get('Developer', row.get('Customer', '')))[:500],
                            'utility': 'NYISO',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Type', row.get('Fuel', ''))),
                            'source': 'NYISO',
                            'source_url': excel_url,
                        }
                        
                        score_result = self.calculate_hunter_score(data)
                        data['hunter_score'] = score_result['hunter_score']
                        data['hunter_notes'] = score_result['hunter_notes']
                        data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"NYISO error: {e}")
        
        logger.info(f"NYISO: Found {len(projects)} projects")
        return projects
    
    # ==================== PJM ====================
    def fetch_pjm(self):
        """PJM - 13 states including Northern Virginia (CRITICAL)"""
        projects = []
        
        try:
            logger.info("PJM: Attempting to find queue file")
            
            # PJM queue page
            page_url = 'https://www.pjm.com/planning/services-requests/interconnection-queues'
            
            response = self.fetch_url(page_url, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find Excel file link
            excel_url = None
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'queue' in href.lower() and ('.xlsx' in href.lower() or '.xls' in href.lower()):
                    excel_url = href
                    if not excel_url.startswith('http'):
                        excel_url = 'https://www.pjm.com' + excel_url
                    break
            
            # Fallback to known URL pattern
            if not excel_url:
                # Try common PJM queue URL patterns
                year = datetime.now().year
                month = datetime.now().strftime('%m')
                excel_url = f'https://www.pjm.com/-/media/planning/services-requests/generation-queue-{year}-{month}.ashx'
            
            logger.info(f"PJM: Trying {excel_url}")
            response = self.fetch_url(excel_url, timeout=30)
            
            if response.status_code == 200:
                df = pd.read_excel(BytesIO(response.content))
                logger.info(f"PJM: Processing {len(df)} rows")
                
                for _, row in df.iterrows():
                    capacity = None
                    for col in ['MW Capacity', 'Capacity (MW)', 'MW', 'Summer Capacity', 'Capacity']:
                        if col in df.columns:
                            capacity = self.extract_capacity(row.get(col))
                            if capacity:
                                break
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        data = {
                            'request_id': f"PJM_{row.get('Queue ID', row.get('Queue Number', 'UNK'))}",
                            'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': str(row.get('State', ''))[:2],
                            'customer': str(row.get('Customer', row.get('Developer', '')))[:500],
                            'utility': 'PJM',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Fuel Type', row.get('Type', ''))),
                            'source': 'PJM',
                            'source_url': excel_url,
                        }
                        
                        score_result = self.calculate_hunter_score(data)
                        data['hunter_score'] = score_result['hunter_score']
                        data['hunter_notes'] = score_result['hunter_notes']
                        data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"PJM error: {e}")
        
        logger.info(f"PJM: Found {len(projects)} projects")
        return projects
    
    # ==================== MISO ====================
    def fetch_miso(self):
        """MISO - 15 states (NEW - CRITICAL GAP)"""
        projects = []
        
        try:
            logger.info("MISO: Fetching queue")
            
            # MISO queue page
            page_url = 'https://www.misoenergy.org/planning/generator-interconnection/GI_Queue/'
            
            response = self.fetch_url(page_url, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find active queue Excel file
            excel_url = None
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'queue' in href.lower() and 'active' in href.lower() and '.xlsx' in href.lower():
                    excel_url = href
                    if not excel_url.startswith('http'):
                        excel_url = 'https://www.misoenergy.org' + excel_url
                    break
            
            if excel_url:
                logger.info(f"MISO: Downloading {excel_url}")
                response = self.fetch_url(excel_url, timeout=30)
                
                if response.status_code == 200:
                    df = pd.read_excel(BytesIO(response.content))
                    logger.info(f"MISO: Processing {len(df)} rows")
                    
                    for _, row in df.iterrows():
                        capacity = None
                        for col in ['MW', 'Capacity (MW)', 'Summer Capacity', 'Capacity']:
                            if col in df.columns:
                                capacity = self.extract_capacity(row.get(col))
                                if capacity:
                                    break
                        
                        if capacity and capacity >= self.min_capacity_mw:
                            data = {
                                'request_id': f"MISO_{row.get('Project Number', row.get('Queue Number', 'UNK'))}",
                                'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                                'capacity_mw': capacity,
                                'county': str(row.get('County', ''))[:200],
                                'state': str(row.get('State', ''))[:2],
                                'customer': str(row.get('Customer', row.get('Developer', '')))[:500],
                                'utility': 'MISO',
                                'status': str(row.get('Status', 'Active')),
                                'fuel_type': str(row.get('Fuel Type', row.get('Type', ''))),
                                'source': 'MISO',
                                'source_url': excel_url,
                            }
                            
                            score_result = self.calculate_hunter_score(data)
                            data['hunter_score'] = score_result['hunter_score']
                            data['hunter_notes'] = score_result['hunter_notes']
                            data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                            
                            data['data_hash'] = self.generate_hash(data)
                            projects.append(data)
                            
        except Exception as e:
            logger.error(f"MISO error: {e}")
        
        logger.info(f"MISO: Found {len(projects)} projects")
        return projects
    
    # ==================== ISO-NE ====================
    def fetch_isone(self):
        """ISO-NE - New England (NEW)"""
        projects = []
        
        try:
            logger.info("ISO-NE: Fetching queue")
            
            # ISO-NE has their queue on their planning page
            page_url = 'https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/interconnection-queue'
            
            response = self.fetch_url(page_url, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the Excel file
            excel_url = None
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'queue' in href.lower() and '.xlsx' in href.lower():
                    excel_url = href
                    if not excel_url.startswith('http'):
                        excel_url = 'https://www.iso-ne.com' + excel_url
                    break
            
            if excel_url:
                logger.info(f"ISO-NE: Downloading {excel_url}")
                response = self.fetch_url(excel_url, timeout=30)
                
                if response.status_code == 200:
                    df = pd.read_excel(BytesIO(response.content))
                    logger.info(f"ISO-NE: Processing {len(df)} rows")
                    
                    for _, row in df.iterrows():
                        capacity = None
                        for col in ['MW Requested', 'Capacity (MW)', 'MW', 'Capacity']:
                            if col in df.columns:
                                capacity = self.extract_capacity(row.get(col))
                                if capacity:
                                    break
                        
                        if capacity and capacity >= self.min_capacity_mw:
                            data = {
                                'request_id': f"ISONE_{row.get('Queue Position', row.get('Project Number', 'UNK'))}",
                                'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                                'capacity_mw': capacity,
                                'county': str(row.get('County', ''))[:200],
                                'state': str(row.get('State', ''))[:2],
                                'customer': str(row.get('Customer', row.get('Developer', '')))[:500],
                                'utility': 'ISO-NE',
                                'status': str(row.get('Status', 'Active')),
                                'fuel_type': str(row.get('Fuel', row.get('Type', ''))),
                                'source': 'ISO-NE',
                                'source_url': excel_url,
                            }
                            
                            score_result = self.calculate_hunter_score(data)
                            data['hunter_score'] = score_result['hunter_score']
                            data['hunter_notes'] = score_result['hunter_notes']
                            data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                            
                            data['data_hash'] = self.generate_hash(data)
                            projects.append(data)
                            
        except Exception as e:
            logger.error(f"ISO-NE error: {e}")
        
        logger.info(f"ISO-NE: Found {len(projects)} projects")
        return projects
    
    # ==================== ERCOT ====================
    def fetch_ercot(self):
        """ERCOT - Texas"""
        projects = []
        
        try:
            logger.info("ERCOT: Fetching queue")
            
            # ERCOT GIS Report
            page_url = 'https://www.ercot.com/gridinfo/generation'
            
            response = self.fetch_url(page_url, timeout=30, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find GIS Report
            excel_url = None
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'gis' in href.lower() and 'report' in href.lower() and '.xlsx' in href.lower():
                    excel_url = href
                    if not excel_url.startswith('http'):
                        excel_url = 'https://www.ercot.com' + href
                    break
            
            if excel_url:
                logger.info(f"ERCOT: Downloading {excel_url}")
                response = self.fetch_url(excel_url, timeout=30, verify=False)
                
                if response.status_code == 200:
                    df = pd.read_excel(BytesIO(response.content))
                    logger.info(f"ERCOT: Processing {len(df)} rows")
                    
                    for _, row in df.iterrows():
                        capacity = None
                        for col in ['INR MW', 'MW', 'Capacity (MW)', 'Capacity']:
                            if col in df.columns:
                                capacity = self.extract_capacity(row.get(col))
                                if capacity:
                                    break
                        
                        if capacity and capacity >= self.min_capacity_mw:
                            data = {
                                'request_id': f"ERCOT_{row.get('Project #', row.get('Project Number', 'UNK'))}",
                                'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                                'capacity_mw': capacity,
                                'county': str(row.get('County', ''))[:200],
                                'state': 'TX',
                                'customer': str(row.get('Company', row.get('Developer', '')))[:500],
                                'utility': 'ERCOT',
                                'status': str(row.get('Status', 'Active')),
                                'fuel_type': str(row.get('Fuel', row.get('Type', ''))),
                                'source': 'ERCOT',
                                'source_url': excel_url,
                            }
                            
                            score_result = self.calculate_hunter_score(data)
                            data['hunter_score'] = score_result['hunter_score']
                            data['hunter_notes'] = score_result['hunter_notes']
                            data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                            
                            data['data_hash'] = self.generate_hash(data)
                            projects.append(data)
                            
        except Exception as e:
            logger.error(f"ERCOT error: {e}")
        
        logger.info(f"ERCOT: Found {len(projects)} projects")
        return projects
    
    # ==================== SPP ====================
    def fetch_spp(self):
        """SPP - Southwest Power Pool"""
        projects = []
        csv_url = 'https://opsportal.spp.org/Studies/GenerateActiveCSV'
        
        try:
            logger.info(f"SPP: Fetching from {csv_url}")
            response = self.fetch_url(csv_url, timeout=30, verify=False)
            
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text))
                logger.info(f"SPP: Processing {len(df)} rows")
                
                for _, row in df.iterrows():
                    capacity = None
                    for col in ['MW', 'Size (MW)', 'Capacity (MW)', 'Capacity']:
                        if col in df.columns:
                            capacity = self.extract_capacity(row.get(col))
                            if capacity:
                                break
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        data = {
                            'request_id': f"SPP_{row.get('Request Number', row.get('GEN-', 'UNK'))}",
                            'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': str(row.get('State', ''))[:2],
                            'customer': str(row.get('Customer', ''))[:500],
                            'utility': 'SPP',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Fuel Type', '')),
                            'source': 'SPP',
                            'source_url': csv_url,
                        }
                        
                        score_result = self.calculate_hunter_score(data)
                        data['hunter_score'] = score_result['hunter_score']
                        data['hunter_notes'] = score_result['hunter_notes']
                        data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"SPP error: {e}")
        
        logger.info(f"SPP: Found {len(projects)} projects")
        return projects
    
    # ==================== MAIN RUNNER ====================
    def run_parallel_monitoring(self, max_workers=3):
        """
        Run monitoring with parallel fetching
        Returns: dict with results
        """
        start_time = time.time()
        all_projects = []
        source_stats = {}
        
        # Define all ISO monitors
        monitors = [
            ('CAISO', self.fetch_caiso),
            ('NYISO', self.fetch_nyiso),
            ('PJM', self.fetch_pjm),          # FIXED
            ('MISO', self.fetch_miso),        # NEW
            ('ISO-NE', self.fetch_isone),     # NEW
            ('ERCOT', self.fetch_ercot),      # IMPROVED
            ('SPP', self.fetch_spp),
        ]
        
        logger.info(f"Starting parallel fetch from {len(monitors)} ISOs...")
        
        # Fetch in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_func): source_name 
                for source_name, fetch_func in monitors
            }
            
            for future in as_completed(futures):
                source_name = futures[future]
                try:
                    projects = future.result(timeout=120)  # 2 minute timeout per source
                    all_projects.extend(projects)
                    source_stats[source_name] = len(projects)
                    logger.info(f"✓ {source_name}: {len(projects)} projects")
                except Exception as e:
                    logger.error(f"✗ {source_name} failed: {e}")
                    source_stats[source_name] = 0
        
        duration = time.time() - start_time
        
        # Calculate statistics
        high_confidence = sum(1 for p in all_projects if p.get('hunter_score', 0) >= 70)
        medium_confidence = sum(1 for p in all_projects if 40 <= p.get('hunter_score', 0) < 70)
        
        return {
            'sources_checked': len(monitors),
            'projects_found': len(all_projects),
            'duration_seconds': round(duration, 2),
            'by_source': source_stats,
            'all_projects': all_projects,
            'statistics': {
                'high_confidence_dc': high_confidence,
                'medium_confidence_dc': medium_confidence,
                'total_capacity_mw': sum(p.get('capacity_mw', 0) for p in all_projects)
            }
        }


# For backward compatibility
HybridPowerMonitor = EnhancedPowerMonitor


if __name__ == '__main__':
    # Test the monitor
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    monitor = EnhancedPowerMonitor()
    results = monitor.run_parallel_monitoring()
    
    print("\n" + "="*60)
    print("MONITORING RESULTS")
    print("="*60)
    print(f"Sources Checked: {results['sources_checked']}")
    print(f"Total Projects: {results['projects_found']}")
    print(f"Duration: {results['duration_seconds']}s")
    print("\nBy Source:")
    for source, count in results['by_source'].items():
        print(f"  {source}: {count}")
    print("\nStatistics:")
    for key, value in results['statistics'].items():
        print(f"  {key}: {value}")
    print("="*60)
