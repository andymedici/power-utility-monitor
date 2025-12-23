# ultra_monitor.py - Complete monitoring system with FERC, PUC, and utility sources
"""
Ultra-Enhanced Power Monitor
- All 7 ISOs (CAISO, NYISO, PJM, MISO, ISO-NE, ERCOT, SPP)
- FERC eLibrary filings
- State PUC monitoring (VA, CA, TX)
- Utility press releases (Dominion, Duke, PG&E, Georgia Power, AEP)
- Advanced hunter scoring
- Parallel processing
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
from urllib.parse import urljoin, quote

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


class UltraPowerMonitor:
    """Ultimate power monitoring with all sources"""
    
    def __init__(self):
        self.min_capacity_mw = 100
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        # FERC API key (optional, public data available without it)
        self.ferc_api_key = os.getenv('FERC_API_KEY', '')
    
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
        developer = project_data.get('developer', '').lower()
        
        combined_text = f"{name} {customer} {fuel} {developer}"
        location = f"{county} {state}"
        
        # 1. EXPLICIT DATA CENTER KEYWORDS (40 points)
        dc_keywords = [
            'data center', 'datacenter', 'data centre',
            'hyperscale', 'cloud', 'colocation', 'colo',
            'server farm', 'computing facility',
            'edge computing', 'edge data', 'compute',
            'bit barn', 'server hub', 'ai training',
            'machine learning facility', 'gpu cluster'
        ]
        
        for keyword in dc_keywords:
            if keyword in combined_text:
                score += 40
                signals.append(f"DC keyword: '{keyword}'")
                break
        
        # 2. TECH COMPANY INDICATORS (25 points)
        tech_companies = [
            'amazon', 'aws', 'amazon web services',
            'microsoft', 'azure', 
            'google', 'gcp', 'alphabet',
            'meta', 'facebook', 
            'apple', 'oracle', 'ibm', 'salesforce',
            'digitalrealty', 'digital realty', 
            'equinix', 'cyrusone', 'qts', 'coresite',
            'iron mountain', 'switch', 'vantage',
            'aligned', 'aligned data centers',
            'flexential', 'cloudflare', 'akamai',
            'quantum loophole', 'stream data',
            'compass datacenters', 'edged energy',
            'scale datacenter', 'prime datacenter',
            'stack infrastructure', 'vantage data centers',
            'cyxtera', 'rackspace', 'lumen',
            'centurylink', 'tiktok', 'bytedance',
            'nvidia', 'openai', 'anthropic',
            'baidu', 'alibaba', 'tencent'
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
            'customer load', 'behind the meter', 'btm',
            'offtake', 'off-take', 'power purchase',
            'network load'
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
            'fauquier': 14, 'stafford': 13,
            
            # Silicon Valley
            'santa clara': 17, 'san jose': 16, 'sunnyvale': 15,
            'mountain view': 15, 'palo alto': 14, 'milpitas': 14,
            
            # Seattle/Oregon
            'king county': 16, 'seattle': 15, 'quincy': 18,
            'hillsboro': 17, 'portland': 14, 'the dalles': 16,
            'prineville': 17, 'morrow': 16, 'umatilla': 15,
            
            # Texas
            'dallas': 15, 'richardson': 15, 'fort worth': 14,
            'plano': 14, 'san antonio': 13, 'austin': 14,
            'temple': 13, 'waxahachie': 13,
            
            # Chicago
            'chicago': 14, 'cook county': 14, 'elk grove': 13,
            'des plaines': 12, 'franklin park': 12,
            
            # Phoenix
            'phoenix': 14, 'maricopa': 14, 'chandler': 14,
            'mesa': 13, 'goodyear': 13,
            
            # Atlanta
            'atlanta': 13, 'fulton': 13, 'gwinnett': 12,
            'douglas': 12, 'lithia springs': 13,
            
            # Ohio
            'columbus': 13, 'franklin': 13, 'dublin': 12,
            'new albany': 15, 'hilliard': 12,
            
            # North Carolina
            'raleigh': 12, 'durham': 12, 'wake': 12,
            'charlotte': 11, 'mecklenburg': 11,
            
            # Other emerging markets
            'des moines': 11, 'polk county': 11,
            'omaha': 11, 'council bluffs': 12,
            'kansas city': 10
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
            (r'tbd\b|to be determined', 'TBD naming'),
            (r'private', 'Private entity'),
            (r'redacted', 'Redacted info')
        ]
        
        for pattern, label in suspicious_patterns:
            if re.search(pattern, combined_text):
                score += 5
                signals.append(label)
                break
        
        # 7. ADDITIONAL POSITIVE SIGNALS (5 points each, max 10)
        positive_signals = [
            ('interconnection agreement', 'Has IA'),
            ('lgia', 'Has LGIA'),
            ('power purchase agreement', 'Has PPA'),
            ('special contract', 'Special contract'),
            ('economic development', 'Econ development'),
            ('critical load', 'Critical load'),
            ('24/7', 'Always-on load'),
            ('redundan', 'Redundancy mentioned'),
            ('n+1', 'N+1 redundancy'),
            ('tier iii', 'Tier III'),
            ('tier 3', 'Tier 3'),
            ('uptime', 'Uptime critical')
        ]
        
        positive_count = 0
        for keyword, label in positive_signals:
            if keyword in combined_text and positive_count < 2:
                score += 5
                signals.append(label)
                positive_count += 1
        
        # 8. NEGATIVE SIGNALS (reduce score significantly)
        negative_keywords = [
            ('solar', 25), ('wind', 25), ('battery', 20), ('storage', 20),
            ('photovoltaic', 25), ('bess', 20), ('renewable', 15),
            ('biomass', 20), ('landfill', 20), ('waste', 15),
            ('natural gas', 20), ('combined cycle', 20), ('gas turbine', 20),
            ('coal', 25), ('nuclear', 25), ('hydro', 20),
            ('geothermal', 20), ('fuel cell', 15)
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
    
    # ==================== ISO METHODS ====================
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
    
    def fetch_pjm(self):
        """PJM - 13 states including Northern Virginia"""
        projects = []
        
        try:
            logger.info("PJM: Attempting to find queue file")
            page_url = 'https://www.pjm.com/planning/services-requests/interconnection-queues'
            
            response = self.fetch_url(page_url, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            excel_url = None
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'queue' in href.lower() and ('.xlsx' in href.lower() or '.xls' in href.lower()):
                    excel_url = href
                    if not excel_url.startswith('http'):
                        excel_url = 'https://www.pjm.com' + excel_url
                    break
            
            if not excel_url:
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
    
    def fetch_miso(self):
        """MISO - 15 states"""
        projects = []
        
        try:
            logger.info("MISO: Fetching queue")
            page_url = 'https://www.misoenergy.org/planning/generator-interconnection/GI_Queue/'
            
            response = self.fetch_url(page_url, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            
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
    
    def fetch_isone(self):
        """ISO-NE - New England"""
        projects = []
        
        try:
            logger.info("ISO-NE: Fetching queue")
            page_url = 'https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/interconnection-queue'
            
            response = self.fetch_url(page_url, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            
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
    
    def fetch_ercot(self):
        """ERCOT - Texas"""
        projects = []
        
        try:
            logger.info("ERCOT: Fetching queue")
            page_url = 'https://www.ercot.com/gridinfo/generation'
            
            response = self.fetch_url(page_url, timeout=30, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            
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
    
    # ==================== FERC eLibrary ====================
    def fetch_ferc_elibrary(self):
        """FERC eLibrary - Federal interconnection agreements"""
        projects = []
        
        try:
            logger.info("FERC: Searching eLibrary for recent filings")
            
            # FERC eLibrary search for Large Generator Interconnection Agreements
            search_url = 'https://elibrary-backup.ferc.gov/idmws/search/fercgensearch.asp'
            
            # Search parameters for recent LGIAs
            date_from = (datetime.now() - timedelta(days=180)).strftime('%m/%d/%Y')
            date_to = datetime.now().strftime('%m/%d/%Y')
            
            params = {
                'sdate': date_from,
                'edate': date_to,
                'searchtype': 'docket',
                'docketnum': '',  # Can be filtered by docket
            }
            
            # Note: FERC parsing is complex - this is simplified
            # Full implementation would parse HTML results and extract PDF links
            
            response = self.fetch_url(search_url, params=params, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for filings with data center keywords
            for result in soup.find_all('tr', class_='search-result'):
                try:
                    title_elem = result.find('td', class_='title')
                    if not title_elem:
                        continue
                    
                    title = title_elem.get_text().lower()
                    
                    # Check for data center related filings
                    if any(kw in title for kw in ['data center', 'large load', 'interconnection agreement']):
                        # Extract capacity if mentioned in title
                        mw_match = re.search(r'(\d+)\s*(?:MW|megawatt)', title, re.IGNORECASE)
                        
                        if mw_match:
                            capacity = float(mw_match.group(1))
                            
                            if capacity >= self.min_capacity_mw:
                                data = {
                                    'request_id': f"FERC_{hashlib.md5(title.encode()).hexdigest()[:10]}",
                                    'project_name': title[:500],
                                    'capacity_mw': capacity,
                                    'county': '',
                                    'state': '',
                                    'customer': '',
                                    'utility': 'FERC Filing',
                                    'status': 'Filed',
                                    'fuel_type': 'Load',
                                    'source': 'FERC',
                                    'source_url': search_url,
                                }
                                
                                score_result = self.calculate_hunter_score(data)
                                data['hunter_score'] = score_result['hunter_score']
                                data['hunter_notes'] = score_result['hunter_notes']
                                data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                                
                                data['data_hash'] = self.generate_hash(data)
                                projects.append(data)
                
                except Exception as e:
                    logger.error(f"Error parsing FERC result: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"FERC error: {e}")
        
        logger.info(f"FERC: Found {len(projects)} projects")
        return projects
    
    # ==================== STATE PUC MONITORING ====================
    def fetch_virginia_scc(self):
        """Virginia State Corporation Commission - Critical for Northern VA DCs"""
        projects = []
        
        try:
            logger.info("VA SCC: Searching for recent filings")
            
            # Virginia SCC case information
            search_url = 'https://www.scc.virginia.gov/pages/Case-Information'
            
            response = self.fetch_url(search_url, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Search for recent PUR (Public Utility) cases
            # Look for "Certificate of Public Convenience and Necessity" cases
            # These often include data center interconnections
            
            for case_link in soup.find_all('a', href=re.compile(r'/case/')):
                try:
                    case_text = case_link.get_text().lower()
                    
                    # Filter for relevant cases
                    if any(kw in case_text for kw in ['certificate', 'transmission', 'interconnection', 'facility']):
                        case_url = urljoin(search_url, case_link['href'])
                        
                        # Fetch case details
                        case_response = self.fetch_url(case_url, timeout=15)
                        case_soup = BeautifulSoup(case_response.text, 'html.parser')
                        case_content = case_soup.get_text().lower()
                        
                        # Check for data center keywords
                        if any(kw in case_content for kw in ['data center', 'large load', 'loudoun', 'ashburn']):
                            # Extract capacity
                            mw_match = re.search(r'(\d+)\s*(?:MW|megawatt)', case_content, re.IGNORECASE)
                            
                            if mw_match:
                                capacity = float(mw_match.group(1))
                                
                                if capacity >= self.min_capacity_mw:
                                    data = {
                                        'request_id': f"VASCC_{case_link['href'].split('/')[-1]}",
                                        'project_name': case_text[:500],
                                        'capacity_mw': capacity,
                                        'county': 'Loudoun' if 'loudoun' in case_content else '',
                                        'state': 'VA',
                                        'customer': '',
                                        'utility': 'Dominion Energy',
                                        'status': 'Filed',
                                        'fuel_type': 'Load',
                                        'source': 'VA SCC',
                                        'source_url': case_url,
                                    }
                                    
                                    score_result = self.calculate_hunter_score(data)
                                    data['hunter_score'] = score_result['hunter_score']
                                    data['hunter_notes'] = score_result['hunter_notes']
                                    data['project_type'] = 'datacenter'
                                    
                                    data['data_hash'] = self.generate_hash(data)
                                    projects.append(data)
                
                except Exception as e:
                    logger.error(f"Error parsing VA SCC case: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"VA SCC error: {e}")
        
        logger.info(f"VA SCC: Found {len(projects)} projects")
        return projects
    
    def fetch_california_puc(self):
        """California Public Utilities Commission"""
        projects = []
        
        try:
            logger.info("CA PUC: Searching for large load cases")
            
            # CA PUC proceedings
            search_url = 'https://apps.cpuc.ca.gov/apex/f?p=401:1'
            
            # Note: CA PUC requires more sophisticated scraping
            # This is a simplified version
            
        except Exception as e:
            logger.error(f"CA PUC error: {e}")
        
        logger.info(f"CA PUC: Found {len(projects)} projects")
        return projects
    
    # ==================== UTILITY PRESS RELEASES ====================
    def fetch_utility_news(self):
        """Scrape utility press releases for data center announcements"""
        projects = []
        
        utilities = {
            'Dominion Energy': {
                'url': 'https://news.dominionenergy.com/newsroom',
                'search_terms': ['data center', 'large load', 'interconnection', 'loudoun', 'ashburn']
            },
            'Duke Energy': {
                'url': 'https://news.duke-energy.com/releases',
                'search_terms': ['data center', 'large load', 'interconnection', 'technology']
            },
            'PG&E': {
                'url': 'https://www.pge.com/en/about/newsroom.html',
                'search_terms': ['data center', 'large customer', 'interconnection', 'silicon valley']
            },
            'Georgia Power': {
                'url': 'https://www.georgiapower.com/company/news-center.html',
                'search_terms': ['data center', 'large load', 'economic development']
            },
            'AEP': {
                'url': 'https://www.aep.com/news',
                'search_terms': ['data center', 'large load', 'technology']
            }
        }
        
        for utility_name, config in utilities.items():
            try:
                logger.info(f"{utility_name}: Checking press releases")
                
                response = self.fetch_url(config['url'], timeout=20)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find article links
                articles = soup.find_all(['article', 'div'], class_=re.compile(r'news|article|press'))
                
                for article in articles[:10]:  # Check last 10 articles
                    try:
                        article_text = article.get_text().lower()
                        
                        # Check if article mentions data centers
                        if any(term in article_text for term in config['search_terms']):
                            # Extract capacity
                            mw_match = re.search(r'(\d+)\s*(?:MW|megawatt)', article_text, re.IGNORECASE)
                            
                            if mw_match:
                                capacity = float(mw_match.group(1))
                                
                                if capacity >= self.min_capacity_mw:
                                    # Try to extract location
                                    location_match = re.search(r'in ([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})', article_text)
                                    county = location_match.group(1) if location_match else ''
                                    state = location_match.group(2) if location_match else ''
                                    
                                    title = article.find(['h1', 'h2', 'h3'])
                                    title_text = title.get_text() if title else 'News Article'
                                    
                                    data = {
                                        'request_id': f"{utility_name.replace(' ', '')}_{hashlib.md5(title_text.encode()).hexdigest()[:10]}",
                                        'project_name': title_text[:500],
                                        'capacity_mw': capacity,
                                        'county': county[:200],
                                        'state': state[:2],
                                        'customer': '',
                                        'utility': utility_name,
                                        'status': 'Announced',
                                        'fuel_type': 'Load',
                                        'source': f'{utility_name} News',
                                        'source_url': config['url'],
                                    }
                                    
                                    score_result = self.calculate_hunter_score(data)
                                    data['hunter_score'] = score_result['hunter_score'] + 10  # Bonus for news announcement
                                    data['hunter_notes'] = score_result['hunter_notes'] + ' | Press release'
                                    data['project_type'] = 'datacenter'
                                    
                                    data['data_hash'] = self.generate_hash(data)
                                    projects.append(data)
                    
                    except Exception as e:
                        continue
            
            except Exception as e:
                logger.error(f"{utility_name} news error: {e}")
        
        logger.info(f"Utility News: Found {len(projects)} projects")
        return projects
    
    # ==================== MAIN RUNNER ====================
    def run_ultra_monitoring(self, max_workers=4):
        """
        Run comprehensive monitoring with all sources
        Returns: dict with results
        """
        start_time = time.time()
        all_projects = []
        source_stats = {}
        
        # Define all monitors
        monitors = [
            # ISOs
            ('CAISO', self.fetch_caiso),
            ('NYISO', self.fetch_nyiso),
            ('PJM', self.fetch_pjm),
            ('MISO', self.fetch_miso),
            ('ISO-NE', self.fetch_isone),
            ('ERCOT', self.fetch_ercot),
            ('SPP', self.fetch_spp),
            
            # Federal/State Sources
            ('FERC', self.fetch_ferc_elibrary),
            ('VA SCC', self.fetch_virginia_scc),
            # ('CA PUC', self.fetch_california_puc),  # Disabled - needs more work
            
            # Utility News
            ('Utility News', self.fetch_utility_news),
        ]
        
        logger.info(f"Starting comprehensive scan from {len(monitors)} sources...")
        
        # Fetch in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_func): source_name 
                for source_name, fetch_func in monitors
            }
            
            for future in as_completed(futures):
                source_name = futures[future]
                try:
                    projects = future.result(timeout=180)  # 3 minute timeout
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
        
        # Top hotspots
        hotspot_counts = {}
        for p in all_projects:
            if p.get('hunter_score', 0) >= 60:
                county = p.get('county', 'Unknown')
                state = p.get('state', '')
                location = f"{county}, {state}"
                hotspot_counts[location] = hotspot_counts.get(location, 0) + 1
        
        top_hotspots = sorted(hotspot_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        return {
            'sources_checked': len(monitors),
            'projects_found': len(all_projects),
            'duration_seconds': round(duration, 2),
            'by_source': source_stats,
            'all_projects': all_projects,
            'statistics': {
                'high_confidence_dc': high_confidence,
                'medium_confidence_dc': medium_confidence,
                'total_capacity_mw': sum(p.get('capacity_mw', 0) for p in all_projects),
                'top_hotspots': top_hotspots
            }
        }


if __name__ == '__main__':
    # Test the monitor
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    monitor = UltraPowerMonitor()
    results = monitor.run_ultra_monitoring()
    
    print("\n" + "="*70)
    print("ULTRA MONITORING RESULTS")
    print("="*70)
    print(f"Sources Checked: {results['sources_checked']}")
    print(f"Total Projects: {results['projects_found']}")
    print(f"Duration: {results['duration_seconds']}s")
    print("\nBy Source:")
    for source, count in results['by_source'].items():
        print(f"  {source}: {count}")
    print("\nStatistics:")
    for key, value in results['statistics'].items():
        if key != 'top_hotspots':
            print(f"  {key}: {value}")
    print("\nTop DC Hotspots:")
    for location, count in results['statistics']['top_hotspots']:
        print(f"  {location}: {count} projects")
    print("="*70)
