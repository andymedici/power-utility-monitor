# HOTFIX_ultra_monitor.py - Fixed version with robust capacity extraction
"""
HOTFIX for Ultra Monitor - Addresses:
1. Capacity extraction failing (0 projects found)
2. FERC URL incorrect
3. PJM Excel format detection
4. Better error logging
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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def extract_capacity(self, value):
        """FIXED: More robust MW capacity extraction"""
        if pd.isna(value) or not value:
            return None
        
        # Convert to string and clean
        text = str(value).replace(',', '').strip().upper()
        
        # Remove common text labels
        text = text.replace('MW', '').replace('MEGAWATT', '').replace('MEGAWATTS', '').strip()
        
        # Try direct conversion
        try:
            capacity = float(text)
            if capacity >= self.min_capacity_mw:
                return capacity
            return None  # Below threshold
        except ValueError:
            pass
        
        # Try extracting first number
        match = re.search(r'(\d+\.?\d*)', text)
        if match:
            try:
                capacity = float(match.group(1))
                if capacity >= self.min_capacity_mw:
                    return capacity
            except ValueError:
                pass
        
        return None
    
    def generate_hash(self, data):
        """Generate unique hash for duplicate detection"""
        key = f"{data.get('project_name', '')}_{data.get('capacity_mw', 0)}_{data.get('location', '')}_{data.get('source', '')}"
        return hashlib.md5(key.lower().encode()).hexdigest()
    
    def calculate_hunter_score(self, project_data):
        """Advanced data center detection scoring (0-100)"""
        score = 0
        signals = []
        
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
            'aligned', 'flexential', 'cloudflare', 'akamai',
            'quantum loophole', 'stream data',
            'compass datacenters', 'edged energy',
            'stack infrastructure', 'cyxtera', 'rackspace',
            'nvidia', 'openai', 'anthropic',
            'baidu', 'alibaba', 'tencent'
        ]
        
        for company in tech_companies:
            if company in combined_text:
                score += 25
                signals.append(f"Tech: {company.title()}")
                break
        
        # 3. CAPACITY SIGNALS (15 points max)
        capacity = project_data.get('capacity_mw', 0)
        
        if capacity >= 500:
            score += 15
            signals.append(f"Very high: {capacity}MW")
        elif capacity >= 300:
            score += 12
            signals.append(f"High: {capacity}MW")
        elif capacity >= 200:
            score += 8
            signals.append(f"Notable: {capacity}MW")
        elif capacity >= 150:
            score += 5
            signals.append(f"Elevated: {capacity}MW")
        
        # 4. FUEL TYPE / LOAD SIGNALS (10 points)
        load_indicators = [
            'load', 'demand', 'behind-meter', 'behind meter',
            'customer load', 'offtake', 'network load'
        ]
        
        for indicator in load_indicators:
            if indicator in fuel.lower():
                score += 10
                signals.append("Load-only")
                break
        
        # 5. GEOGRAPHIC HOTSPOTS (20 points max)
        dc_hotspots = {
            'loudoun': 20, 'ashburn': 20, 'leesburg': 18, 
            'fairfax': 18, 'prince william': 17,
            'santa clara': 17, 'san jose': 16,
            'king county': 16, 'seattle': 15, 'quincy': 18,
            'hillsboro': 17, 'portland': 14,
            'dallas': 15, 'richardson': 15,
            'chicago': 14, 'cook county': 14,
            'phoenix': 14, 'maricopa': 14, 'chandler': 14,
            'atlanta': 13, 'fulton': 13,
            'columbus': 13, 'franklin': 13, 'new albany': 15,
        }
        
        location_lower = location.lower()
        for place, points in dc_hotspots.items():
            if place in location_lower:
                score += points
                signals.append(f"Hotspot: {place.title()}")
                break
        
        # 6. SUSPICIOUS NAMING PATTERNS (10 points)
        suspicious_patterns = [
            (r'project [a-z]?\d+', 'Generic naming'),
            (r'facility [a-z]', 'Facility code'),
            (r'campus [a-z]?\d*', 'Campus naming'),
            (r'confidential', 'Confidential'),
        ]
        
        for pattern, label in suspicious_patterns:
            if re.search(pattern, combined_text):
                score += 5
                signals.append(label)
                break
        
        # 7. NEGATIVE SIGNALS (reduce score)
        negative_keywords = [
            ('solar', 25), ('wind', 25), ('battery', 20),
            ('photovoltaic', 25), ('renewable', 15),
        ]
        
        for keyword, penalty in negative_keywords:
            if keyword in combined_text:
                score = max(0, score - penalty)
                signals.append(f"Not DC: {keyword}")
                break
        
        score = min(100, max(0, score))
        
        return {
            'hunter_score': score,
            'hunter_notes': ' | '.join(signals[:3]) if signals else 'No signals',
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
                
                # Log column names for debugging
                logger.info(f"CAISO columns: {list(df.columns)[:10]}")
                
                for idx, row in df.iterrows():
                    # Try multiple possible capacity column names
                    capacity = None
                    capacity_cols = ['MW', 'Capacity (MW)', 'Max Output (MW)', 'Capacity', 'Summer Capacity (MW)', 'Winter Capacity (MW)']
                    
                    for col in capacity_cols:
                        if col in df.columns:
                            cap = self.extract_capacity(row.get(col))
                            if cap:
                                capacity = cap
                                break
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        data = {
                            'request_id': f"CAISO_{row.get('Queue Number', row.get('Queue ID', f'ROW{idx}'))}",
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
                
                logger.info(f"CAISO: Found {len(projects)} projects (>= {self.min_capacity_mw}MW)")
                        
        except Exception as e:
            logger.error(f"CAISO error: {e}", exc_info=True)
        
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
                logger.info(f"NYISO columns: {list(df.columns)[:10]}")
                
                for idx, row in df.iterrows():
                    capacity = None
                    capacity_cols = ['MW', 'Capacity (MW)', 'Summer Cap', 'Winter Cap', 'Capacity']
                    
                    for col in capacity_cols:
                        if col in df.columns:
                            cap = self.extract_capacity(row.get(col))
                            if cap:
                                capacity = cap
                                break
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        data = {
                            'request_id': f"NYISO_{row.get('Queue Pos.', row.get('Queue Position', f'ROW{idx}'))}",
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
            logger.error(f"NYISO error: {e}", exc_info=True)
        
        logger.info(f"NYISO: Found {len(projects)} projects")
        return projects
    
    # ==================== PJM ====================
    def fetch_pjm(self):
        """PJM - FIXED: Better URL detection and Excel handling"""
        projects = []
        
        try:
            logger.info("PJM: Attempting to find queue file")
            
            # Try known direct URLs first
            possible_urls = [
                'https://www.pjm.com/-/media/planning/services-requests/interconnection-queues.ashx',
                'https://pjm.com/-/media/planning/services-requests/gen-interconnection-queue.xlsx',
                'https://www.pjm.com/-/media/planning/services-requests/new-services-queue.ashx',
            ]
            
            for url in possible_urls:
                try:
                    logger.info(f"PJM: Trying {url}")
                    response = self.fetch_url(url, timeout=30)
                    
                    if response.status_code == 200 and len(response.content) > 1000:
                        # Try to read as Excel
                        try:
                            df = pd.read_excel(BytesIO(response.content), engine='openpyxl')
                            logger.info(f"PJM: Successfully loaded Excel with {len(df)} rows")
                            logger.info(f"PJM columns: {list(df.columns)[:10]}")
                            
                            for idx, row in df.iterrows():
                                capacity = None
                                capacity_cols = ['MW Capacity', 'Capacity (MW)', 'MW', 'Summer Capacity', 'Capacity', 'Max MW']
                                
                                for col in capacity_cols:
                                    if col in df.columns:
                                        cap = self.extract_capacity(row.get(col))
                                        if cap:
                                            capacity = cap
                                            break
                                
                                if capacity and capacity >= self.min_capacity_mw:
                                    data = {
                                        'request_id': f"PJM_{row.get('Queue ID', row.get('Queue Number', f'ROW{idx}'))}",
                                        'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                                        'capacity_mw': capacity,
                                        'county': str(row.get('County', ''))[:200],
                                        'state': str(row.get('State', ''))[:2],
                                        'customer': str(row.get('Customer', row.get('Developer', '')))[:500],
                                        'utility': 'PJM',
                                        'status': str(row.get('Status', 'Active')),
                                        'fuel_type': str(row.get('Fuel Type', row.get('Type', ''))),
                                        'source': 'PJM',
                                        'source_url': url,
                                    }
                                    
                                    score_result = self.calculate_hunter_score(data)
                                    data['hunter_score'] = score_result['hunter_score']
                                    data['hunter_notes'] = score_result['hunter_notes']
                                    data['project_type'] = 'datacenter' if score_result['hunter_score'] >= 60 else 'other'
                                    data['data_hash'] = self.generate_hash(data)
                                    projects.append(data)
                            
                            logger.info(f"PJM: Found {len(projects)} projects from {url}")
                            break  # Success, exit loop
                            
                        except Exception as e:
                            logger.warning(f"PJM: Failed to parse {url} as Excel: {e}")
                            continue
                
                except Exception as e:
                    logger.warning(f"PJM: Failed to fetch {url}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"PJM error: {e}", exc_info=True)
        
        logger.info(f"PJM: Found {len(projects)} projects total")
        return projects
    
    # ==================== Simplified other ISOs for now ====================
    def fetch_miso(self):
        """MISO - Simplified"""
        logger.info("MISO: Skipping for hotfix (needs URL verification)")
        return []
    
    def fetch_isone(self):
        """ISO-NE - Simplified"""
        logger.info("ISO-NE: Skipping for hotfix (needs URL verification)")
        return []
    
    def fetch_ercot(self):
        """ERCOT - Simplified"""
        logger.info("ERCOT: Skipping for hotfix (needs URL verification)")
        return []
    
    def fetch_spp(self):
        """SPP - FIXED"""
        projects = []
        csv_url = 'https://opsportal.spp.org/Studies/GenerateActiveCSV'
        
        try:
            logger.info(f"SPP: Fetching from {csv_url}")
            response = self.fetch_url(csv_url, timeout=30, verify=False)
            
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text))
                logger.info(f"SPP: Processing {len(df)} rows")
                logger.info(f"SPP columns: {list(df.columns)[:10]}")
                
                for idx, row in df.iterrows():
                    capacity = None
                    capacity_cols = ['MW', 'Size (MW)', 'Capacity (MW)', 'Capacity', 'Summer MW', 'Winter MW']
                    
                    for col in capacity_cols:
                        if col in df.columns:
                            cap = self.extract_capacity(row.get(col))
                            if cap:
                                capacity = cap
                                break
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        data = {
                            'request_id': f"SPP_{row.get('Request Number', row.get('GEN-', f'ROW{idx}'))}",
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
            logger.error(f"SPP error: {e}", exc_info=True)
        
        logger.info(f"SPP: Found {len(projects)} projects")
        return projects
    
    # ==================== Simplified FERC/PUC/News for hotfix ====================
    def fetch_ferc_elibrary(self):
        """FERC - Disabled for hotfix (URL incorrect)"""
        logger.info("FERC: Skipping for hotfix (needs correct URL)")
        return []
    
    def fetch_virginia_scc(self):
        """VA SCC - Disabled for hotfix"""
        logger.info("VA SCC: Skipping for hotfix")
        return []
    
    def fetch_utility_news(self):
        """Utility News - Disabled for hotfix"""
        logger.info("Utility News: Skipping for hotfix")
        return []
    
    # ==================== MAIN RUNNER ====================
    def run_ultra_monitoring(self, max_workers=4):
        """Run monitoring with critical sources only (hotfix)"""
        start_time = time.time()
        all_projects = []
        source_stats = {}
        
        # HOTFIX: Only run sources that are confirmed working
        monitors = [
            ('CAISO', self.fetch_caiso),
            ('NYISO', self.fetch_nyiso),
            ('PJM', self.fetch_pjm),
            ('SPP', self.fetch_spp),
        ]
        
        logger.info(f"HOTFIX: Running {len(monitors)} critical sources...")
        
        for source_name, fetch_func in monitors:
            try:
                logger.info(f"Fetching from {source_name}...")
                source_projects = fetch_func()
                all_projects.extend(source_projects)
                source_stats[source_name] = len(source_projects)
                logger.info(f"✓ {source_name}: {len(source_projects)} projects")
            except Exception as e:
                logger.error(f"✗ {source_name} failed: {e}")
                source_stats[source_name] = 0
        
        duration = time.time() - start_time
        
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
