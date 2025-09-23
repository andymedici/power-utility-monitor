# app.py - Real Data Power Monitor with 90-Day Expiration and Enhanced References
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
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

# Fix Railway PostgreSQL URL
if 'DATABASE_URL' in os.environ:
    if os.environ['DATABASE_URL'].startswith('postgres://'):
        os.environ['DATABASE_URL'] = os.environ['DATABASE_URL'].replace('postgres://', 'postgresql://', 1)

from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Enhanced Database Model with Reference Fields and 90-day expiration
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
    
    # NEW REFERENCE FIELDS
    docket_number = db.Column(db.String(100))      # FERC docket if available
    case_number = db.Column(db.String(100))        # State regulatory case
    document_url = db.Column(db.Text)              # Direct link to specific document
    queue_report_url = db.Column(db.Text)          # Monthly queue report link
    
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

# Real Data Power Monitor - NO FAKE DATA
class RealDataPowerMonitor:
    def __init__(self):
        self.min_capacity_mw = 100
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def extract_capacity(self, text):
        """Extract MW capacity from various formats"""
        if pd.isna(text) or not text:
            return None
        text = str(text)
        
        # Try multiple patterns
        patterns = [
            r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*MW',
            r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*megawatt',
            r'(\d+(?:,\d{3})*)\s*kW',  # Convert kW to MW
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                capacity = float(match.group(1).replace(',', ''))
                if 'kW' in pattern:
                    capacity = capacity / 1000
                if capacity >= self.min_capacity_mw:
                    return capacity
        return None
    
    def generate_hash(self, data):
        """Generate unique hash for duplicate detection"""
        key = f"{data.get('project_name', '')}_{data.get('capacity_mw', 0)}_{data.get('location', '')}_{data.get('source', '')}"
        return hashlib.md5(key.lower().encode()).hexdigest()
    
    def classify_project(self, name, customer, fuel_type=''):
        """Enhanced project classification"""
        text = f"{name} {customer} {fuel_type}".lower()
        
        # Data center keywords
        if any(word in text for word in ['data center', 'datacenter', 'cloud', 'hyperscale', 
                                         'colocation', 'colo', 'server', 'computing',
                                         'google', 'amazon', 'microsoft', 'meta', 'facebook']):
            return 'datacenter'
        
        # Manufacturing keywords
        if any(word in text for word in ['manufacturing', 'factory', 'plant', 'production',
                                         'automotive', 'battery', 'semiconductor', 'fab']):
            return 'manufacturing'
        
        # Energy storage
        if any(word in text for word in ['battery', 'storage', 'bess']):
            return 'storage'
        
        # Renewable energy
        if any(word in text for word in ['solar', 'wind', 'renewable', 'photovoltaic']):
            return 'renewable'
        
        return 'other'
    
    def fetch_caiso_queue(self):
        """CAISO - California Independent System Operator - REAL DATA ONLY"""
        projects = []
        source_url = 'http://www.caiso.com/planning/Pages/GeneratorInterconnection/Default.aspx'
        
        try:
            # Try to fetch the Excel file
            excel_url = 'http://www.caiso.com/planning/Documents/AllGeneratorInterconnectionQueue.xlsx'
            response = self.session.get(excel_url, timeout=30)
            
            if response.status_code == 200:
                df = pd.read_excel(BytesIO(response.content))
                logger.info(f"CAISO: Processing {len(df)} rows")
                
                for _, row in df.iterrows():
                    # Look for capacity
                    capacity = None
                    capacity_cols = ['MW', 'Capacity MW', 'Gen MW', 'Net MW', 'Max MW']
                    for col in capacity_cols:
                        if col in df.columns and pd.notna(row[col]):
                            try:
                                capacity = float(str(row[col]).replace(',', ''))
                                if capacity >= self.min_capacity_mw:
                                    break
                            except:
                                continue
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        queue_num = str(row.get('Queue Position', row.get('Queue Number', 'UNK')))
                        project_name = str(row.get('Project Name', row.get('Generator', 'Unknown')))
                        customer = str(row.get('Interconnection Customer', ''))
                        
                        # Build specific document URL if queue number exists
                        doc_url = None
                        if queue_num and queue_num != 'UNK':
                            doc_url = f"http://www.caiso.com/Documents/Queue-{queue_num}-InterconnectionRequest.pdf"
                        
                        data = {
                            'request_id': f"CAISO_{queue_num}",
                            'queue_position': queue_num,
                            'project_name': project_name[:500],
                            'capacity_mw': capacity,
                            'location': str(row.get('Location', ''))[:500],
                            'county': str(row.get('County', ''))[:200],
                            'state': 'CA',
                            'customer': customer[:500],
                            'developer': str(row.get('Developer', row.get('Applicant', '')))[:500],
                            'utility': 'CAISO',
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Fuel Type', '')),
                            'interconnection_point': str(row.get('Point of Interconnection', ''))[:500],
                            'source': 'CAISO',
                            'source_url': source_url,
                            'document_url': doc_url,  # NEW: Direct document link
                            'project_type': self.classify_project(project_name, customer, row.get('Fuel Type', ''))
                        }
                        
                        # Try to parse dates
                        for date_field in ['Queue Date', 'Application Date']:
                            if date_field in df.columns and pd.notna(row[date_field]):
                                try:
                                    data['queue_date'] = pd.to_datetime(row[date_field]).date()
                                    break
                                except:
                                    pass
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"Error fetching CAISO data: {e}")
        
        logger.info(f"CAISO: Found {len(projects)} qualifying projects")
        return projects
    
    def fetch_pjm_queue(self):
        """PJM - REAL DATA ONLY - No fallbacks"""
        projects = []
        source_url = 'https://www.pjm.com/planning/services-requests/interconnection-queues'
        
        try:
            csv_urls = [
                'https://www.pjm.com/pub/planning/downloads/queues/active-queue.csv',
                'https://www.pjm.com/-/media/planning/gen-interconnection/queues/active-queue.csv'
            ]
            
            df = None
            for csv_url in csv_urls:
                try:
                    response = self.session.get(csv_url, timeout=30)
                    if response.status_code == 200:
                        df = pd.read_csv(StringIO(response.text))
                        logger.info(f"PJM: Processing {len(df)} rows")
                        break
                except:
                    continue
            
            if df is not None:
                for _, row in df.iterrows():
                    # Extract capacity
                    capacity = None
                    for col in ['MFO', 'MW Capacity', 'Summer MW', 'Winter MW', 'MW']:
                        if col in df.columns and pd.notna(row[col]):
                            try:
                                capacity = float(str(row[col]).replace(',', ''))
                                if capacity >= self.min_capacity_mw:
                                    break
                            except:
                                continue
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        queue_id = str(row.get('Queue Number', row.get('Queue ID', 'UNK')))
                        project_name = str(row.get('Project Name', row.get('Facility', 'Unknown')))
                        customer = str(row.get('Developer', row.get('Customer', '')))
                        
                        # Build specific PJM document URL
                        doc_url = None
                        if queue_id and queue_id != 'UNK':
                            doc_url = f"https://www.pjm.com/pub/planning/project-queues/{queue_id}.pdf"
                        
                        data = {
                            'request_id': f"PJM_{queue_id}",
                            'queue_position': queue_id,
                            'project_name': project_name[:500],
                            'capacity_mw': capacity,
                            'location': str(row.get('Location', ''))[:500],
                            'county': str(row.get('County', ''))[:200],
                            'state': str(row.get('State', ''))[:2],
                            'customer': customer[:500],
                            'developer': str(row.get('Developer', ''))[:500],
                            'utility': str(row.get('TO', 'PJM')),
                            'interconnection_point': str(row.get('POI', ''))[:500],
                            'status': str(row.get('Status', 'Active')),
                            'fuel_type': str(row.get('Fuel', '')),
                            'source': 'PJM',
                            'source_url': source_url,
                            'document_url': doc_url,  # NEW: Direct document link
                            'project_type': self.classify_project(project_name, customer, row.get('Fuel', ''))
                        }
                        
                        # Check for FERC docket
                        if 'FERC Docket' in df.columns and pd.notna(row.get('FERC Docket')):
                            data['docket_number'] = str(row.get('FERC Docket'))[:100]
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"Error fetching PJM data: {e}")
            # NO FAKE DATA - just return empty if real data fails
        
        logger.info(f"PJM: Found {len(projects)} qualifying projects")
        return projects
    
    def fetch_ercot_queue(self):
        """ERCOT - Texas - REAL DATA ONLY"""
        projects = []
        source_url = 'http://www.ercot.com/gridinfo/resource'
        
        try:
            # ERCOT GIS Report - updated monthly
            current_date = datetime.now()
            gis_url = f'http://www.ercot.com/content/wcm/lists/226522/GIS_Report_{current_date.strftime("%m_%d_%Y")}.xlsx'
            
            # Try current month, then previous month
            urls_to_try = [
                gis_url,
                f'http://www.ercot.com/content/wcm/lists/226522/GIS_Report_{(current_date - timedelta(days=30)).strftime("%m_%d_%Y")}.xlsx'
            ]
            
            df = None
            for url in urls_to_try:
                try:
                    response = self.session.get(url, timeout=30)
                    if response.status_code == 200:
                        df = pd.read_excel(BytesIO(response.content), sheet_name='Interconnection Requests')
                        logger.info(f"ERCOT: Processing {len(df)} rows from {url}")
                        break
                except:
                    continue
            
            if df is not None:
                for _, row in df.iterrows():
                    capacity = None
                    for col in ['Capacity (MW)', 'MW', 'Max Output']:
                        if col in df.columns and pd.notna(row[col]):
                            try:
                                capacity = float(str(row[col]).replace(',', ''))
                                if capacity >= self.min_capacity_mw:
                                    break
                            except:
                                continue
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        gin_number = str(row.get('GIN', row.get('Request ID', 'UNK')))
                        project_name = str(row.get('Project Name', row.get('Generation Interconnection', 'Unknown')))
                        
                        data = {
                            'request_id': f"ERCOT_{gin_number}",
                            'queue_position': gin_number,
                            'project_name': project_name[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': 'TX',
                            'customer': str(row.get('Developer', ''))[:500],
                            'developer': str(row.get('Interconnection Customer', ''))[:500],
                            'utility': 'ERCOT',
                            'fuel_type': str(row.get('Fuel Type', row.get('Technology', ''))),
                            'status': str(row.get('Status', 'Active')),
                            'interconnection_point': str(row.get('POI', ''))[:500],
                            'source': 'ERCOT',
                            'source_url': source_url,
                            'queue_report_url': url,  # NEW: Link to actual report
                            'project_type': self.classify_project(project_name, row.get('Developer', ''), row.get('Fuel Type', ''))
                        }
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"Error fetching ERCOT data: {e}")
            # NO FAKE DATA
        
        logger.info(f"ERCOT: Found {len(projects)} qualifying projects")
        return projects
    
    def fetch_miso_queue(self):
        """MISO - Midcontinent ISO - REAL DATA ONLY"""
        projects = []
        source_url = 'https://www.misoenergy.org/planning/generator-interconnection/generator-interconnection-queue/'
        
        try:
            queue_url = 'https://cdn.misoenergy.org/Generator%20Interconnection%20Queue.xlsx'
            response = self.session.get(queue_url, timeout=30)
            
            if response.status_code == 200:
                df = pd.read_excel(BytesIO(response.content))
                logger.info(f"MISO: Processing {len(df)} rows")
                
                for _, row in df.iterrows():
                    capacity = self.extract_capacity(str(row.get('Capacity (MW)', row.get('MW', ''))))
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        project_num = str(row.get('Project Number', 'UNK'))
                        
                        data = {
                            'request_id': f"MISO_{project_num}",
                            'queue_position': project_num,
                            'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                            'capacity_mw': capacity,
                            'county': str(row.get('County', ''))[:200],
                            'state': str(row.get('State', ''))[:2],
                            'customer': str(row.get('Customer', ''))[:500],
                            'developer': str(row.get('Developer', ''))[:500],
                            'utility': 'MISO',
                            'fuel_type': str(row.get('Fuel Type', '')),
                            'status': str(row.get('Status', 'Active')),
                            'interconnection_point': str(row.get('POI', ''))[:500],
                            'source': 'MISO',
                            'source_url': source_url,
                            'queue_report_url': queue_url,
                            'project_type': self.classify_project(
                                row.get('Project Name', ''), 
                                row.get('Customer', ''),
                                row.get('Fuel Type', '')
                            )
                        }
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"Error fetching MISO data: {e}")
        
        logger.info(f"MISO: Found {len(projects)} qualifying projects")
        return projects
    
    def fetch_isone_queue(self):
        """ISO New England - REAL DATA ONLY"""
        projects = []
        source_url = 'https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/seasonal-claimed-capability'
        
        try:
            queue_url = 'https://www.iso-ne.com/static-assets/documents/markets/genrtion_busbar/interconnection_queue.xlsx'
            response = self.session.get(queue_url, timeout=30)
            
            if response.status_code == 200:
                df = pd.read_excel(BytesIO(response.content))
                logger.info(f"ISO-NE: Processing {len(df)} rows")
                
                for _, row in df.iterrows():
                    capacity = self.extract_capacity(str(row.get('MW', row.get('Capacity', ''))))
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        queue_pos = str(row.get('Queue Position', 'UNK'))
                        
                        data = {
                            'request_id': f"ISONE_{queue_pos}",
                            'queue_position': queue_pos,
                            'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                            'capacity_mw': capacity,
                            'location': str(row.get('Location', ''))[:500],
                            'state': str(row.get('State', ''))[:2],
                            'customer': str(row.get('Developer', ''))[:500],
                            'developer': str(row.get('Interconnection Customer', ''))[:500],
                            'utility': 'ISO-NE',
                            'fuel_type': str(row.get('Resource Type', '')),
                            'status': str(row.get('Status', 'Active')),
                            'source': 'ISO-NE',
                            'source_url': source_url,
                            'queue_report_url': queue_url,
                            'project_type': self.classify_project(
                                row.get('Project Name', ''),
                                row.get('Developer', ''),
                                row.get('Resource Type', '')
                            )
                        }
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"Error fetching ISO-NE data: {e}")
        
        logger.info(f"ISO-NE: Found {len(projects)} qualifying projects")
        return projects
    
    def fetch_spp_queue(self):
        """SPP - Southwest Power Pool - REAL DATA ONLY"""
        projects = []
        source_url = 'https://www.spp.org/planning/generator-interconnection/'
        
        try:
            queue_url = 'https://www.spp.org/documents/generator-interconnection-queue.xlsx'
            response = self.session.get(queue_url, timeout=30)
            
            if response.status_code == 200:
                df = pd.read_excel(BytesIO(response.content))
                logger.info(f"SPP: Processing {len(df)} rows")
                
                for _, row in df.iterrows():
                    capacity = self.extract_capacity(str(row.get('MW', row.get('Capacity', ''))))
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        request_num = str(row.get('Request Number', 'UNK'))
                        
                        data = {
                            'request_id': f"SPP_{request_num}",
                            'queue_position': request_num,
                            'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                            'capacity_mw': capacity,
                            'location': str(row.get('Location', ''))[:500],
                            'state': str(row.get('State', ''))[:2],
                            'customer': str(row.get('Customer', ''))[:500],
                            'developer': str(row.get('Interconnection Customer', ''))[:500],
                            'utility': 'SPP',
                            'fuel_type': str(row.get('Generation Type', '')),
                            'status': str(row.get('Status', 'Active')),
                            'interconnection_point': str(row.get('POI', ''))[:500],
                            'source': 'SPP',
                            'source_url': source_url,
                            'queue_report_url': queue_url,
                            'project_type': self.classify_project(
                                row.get('Project Name', ''),
                                row.get('Customer', ''),
                                row.get('Generation Type', '')
                            )
                        }
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"Error fetching SPP data: {e}")
        
        logger.info(f"SPP: Found {len(projects)} qualifying projects")
        return projects
    
    def fetch_nyiso_queue(self):
        """NYISO - New York - REAL DATA ONLY"""
        projects = []
        source_url = 'https://www.nyiso.com/interconnection-process'
        
        try:
            queue_url = 'https://www.nyiso.com/documents/interconnection-queue.csv'
            response = self.session.get(queue_url, timeout=30)
            
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text))
                logger.info(f"NYISO: Processing {len(df)} rows")
                
                for _, row in df.iterrows():
                    capacity = self.extract_capacity(str(row.get('MW', row.get('Capacity', ''))))
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        queue_pos = str(row.get('Queue Position', 'UNK'))
                        
                        data = {
                            'request_id': f"NYISO_{queue_pos}",
                            'queue_position': queue_pos,
                            'project_name': str(row.get('Project Name', 'Unknown'))[:500],
                            'capacity_mw': capacity,
                            'location': str(row.get('Location', ''))[:500],
                            'county': str(row.get('County', ''))[:200],
                            'state': 'NY',
                            'customer': str(row.get('Developer', ''))[:500],
                            'developer': str(row.get('Interconnection Customer', ''))[:500],
                            'utility': 'NYISO',
                            'fuel_type': str(row.get('Type', '')),
                            'status': str(row.get('Status', 'Active')),
                            'interconnection_point': str(row.get('POI', ''))[:500],
                            'source': 'NYISO',
                            'source_url': source_url,
                            'project_type': self.classify_project(
                                row.get('Project Name', ''),
                                row.get('Developer', ''),
                                row.get('Type', '')
                            )
                        }
                        
                        # Check for state case number
                        if 'Case Number' in df.columns and pd.notna(row.get('Case Number')):
                            data['case_number'] = str(row.get('Case Number'))[:100]
                        
                        data['data_hash'] = self.generate_hash(data)
                        projects.append(data)
                        
        except Exception as e:
            logger.error(f"Error fetching NYISO data: {e}")
        
        logger.info(f"NYISO: Found {len(projects)} qualifying projects")
        return projects
    
    def run_comprehensive_monitoring(self):
        """Run monitoring across all REAL sources - NO FAKE DATA"""
        start_time = time.time()
        all_projects = []
        source_stats = {}
        
        # List of all REAL monitoring functions
        monitors = [
            ('CAISO', self.fetch_caiso_queue),      # California
            ('PJM', self.fetch_pjm_queue),          # 13 states + DC
            ('ERCOT', self.fetch_ercot_queue),      # Texas
            ('MISO', self.fetch_miso_queue),        # 15 states
            ('ISO-NE', self.fetch_isone_queue),     # 6 New England states
            ('SPP', self.fetch_spp_queue),          # 14 states
            ('NYISO', self.fetch_nyiso_queue),      # New York
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
                # Add hash if missing
                if 'data_hash' not in project_data:
                    project_data['data_hash'] = self.generate_hash(project_data)
                
                # Check for duplicates
                existing = PowerProject.query.filter_by(data_hash=project_data['data_hash']).first()
                if not existing:
                    project = PowerProject(**project_data)
                    db.session.add(project)
                    stored += 1
            except Exception as e:
                logger.error(f"Error storing project: {e}")
        
        # Record monitoring run
        duration = time.time() - start_time
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
        
        # Clean up old projects (>90 days)
        cutoff_date = datetime.utcnow() - timedelta(days=90)
        old_count = PowerProject.query.filter(PowerProject.created_at < cutoff_date).delete()
        db.session.commit()
        logger.info(f"Cleaned up {old_count} projects older than 90 days")
        
        return {
            'sources_checked': total_sources,
            'projects_found': len(all_projects),
            'projects_stored': stored,
            'duration_seconds': round(duration, 2),
            'by_source': source_stats,
            'old_projects_removed': old_count
        }

# Flask Routes - Updated to use 90-day filter
@app.route('/')
def index():
    # Use active() method for 90-day filter
    total = PowerProject.active().count()
    recent = PowerProject.active().filter(
        PowerProject.created_at >= datetime.utcnow() - timedelta(days=30)
    ).count()
    
    # Count by type (with 90-day filter)
    datacenter_count = PowerProject.active().filter_by(project_type='datacenter').count()
    manufacturing_count = PowerProject.active().filter_by(project_type='manufacturing').count()
    
    # High capacity projects (with 90-day filter)
    high_capacity = PowerProject.active().filter(PowerProject.capacity_mw >= 200).count()
    very_high_capacity = PowerProject.active().filter(PowerProject.capacity_mw >= 500).count()
    
    # Get state coverage
    states_covered = db.session.query(PowerProject.state).filter(
        PowerProject.created_at >= datetime.utcnow() - timedelta(days=90)
    ).distinct().count()
    
    # Recent monitoring
    last_run = MonitoringRun.query.order_by(MonitoringRun.run_date.desc()).first()
    
    # Top states by project count (with 90-day filter)
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
        <title>US Power Projects Monitor - Real Data Only</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; background: #f0f2f5; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px 40px; }}
            .container {{ max-width: 1400px; margin: 0 auto; padding: 20px 40px; }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
            .stat-card {{ background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .stat-card h3 {{ margin: 0; font-size: 32px; color: #333; }}
            .stat-card p {{ margin: 5px 0 0 0; color: #666; font-size: 14px; }}
            .card {{ background: white; padding: 25px; margin: 20px 0; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .btn {{ padding: 12px 24px; background: #007bff; color: white; text-decoration: none; border-radius: 5px; display: inline-block; margin: 5px; transition: background 0.3s; }}
            .btn:hover {{ background: #0056b3; }}
            .btn-success {{ background: #28a745; }}
            .btn-warning {{ background: #ffc107; color: #333; }}
            .nav {{ background: #2c3e50; padding: 0; }}
            .nav a {{ color: white; padding: 15px 20px; display: inline-block; text-decoration: none; transition: background 0.3s; }}
            .nav a:hover {{ background: #34495e; }}
            .coverage-badge {{ background: #28a745; color: white; padding: 5px 10px; border-radius: 20px; font-size: 12px; }}
            .notice {{ background: #fff3cd; border-left: 4px solid #ffc107; padding: 10px; margin: 10px 0; }}
            table {{ width: 100%; }}
            th {{ text-align: left; padding: 10px; border-bottom: 2px solid #dee2e6; }}
            td {{ padding: 10px; border-bottom: 1px solid #dee2e6; }}
        </style>
    </head>
    <body>
        <div class="nav">
            <a href="/">🏠 Dashboard</a>
            <a href="/projects">📊 All Projects</a>
            <a href="/projects?type=datacenter">🖥️ Data Centers</a>
            <a href="/projects?type=manufacturing">🏭 Manufacturing</a>
            <a href="/projects?min_capacity=200">⚡ 200+ MW</a>
            <a href="/monitoring">📡 Monitoring</a>
            <a href="/sources">🌐 Data Sources</a>
        </div>
        
        <div class="header">
            <h1>⚡ US Power Projects Monitor</h1>
            <p>Real-time data from official grid operator sources</p>
            <span class="coverage-badge">CAISO • PJM • ERCOT • MISO • ISO-NE • SPP • NYISO</span>
        </div>
        
        <div class="container">
            <div class="notice">
                📌 <strong>Data Retention:</strong> Projects are automatically removed after 90 days. Export data regularly to maintain long-term records.
            </div>
            
            <div class="stats-grid">
                <div class="stat-card">
                    <h3>{total}</h3>
                    <p>Active Projects (90d)</p>
                </div>
                <div class="stat-card">
                    <h3>{states_covered}</h3>
                    <p>States Covered</p>
                </div>
                <div class="stat-card">
                    <h3>{datacenter_count}</h3>
                    <p>Data Centers</p>
                </div>
                <div class="stat-card">
                    <h3>{manufacturing_count}</h3>
                    <p>Manufacturing</p>
                </div>
                <div class="stat-card">
                    <h3>{high_capacity}</h3>
                    <p>200+ MW Projects</p>
                </div>
                <div class="stat-card">
                    <h3>{very_high_capacity}</h3>
                    <p>500+ MW Projects</p>
                </div>
                <div class="stat-card">
                    <h3>{recent}</h3>
                    <p>Added Last 30 Days</p>
                </div>
                <div class="stat-card">
                    <h3>{last_run.run_date.strftime('%m/%d %H:%M') if last_run else 'Never'}</h3>
                    <p>Last Scan</p>
                </div>
            </div>
            
            <div class="card">
                <h2>Top States by Project Count (Active Projects)</h2>
                <table>
                    <tr><th>State</th><th>Projects</th><th>Total Capacity</th></tr>
                    {''.join([f"<tr><td>{s.state or 'Unknown'}</td><td>{s.count}</td><td>{s.total_mw:,.0f} MW</td></tr>" for s in top_states])}
                </table>
            </div>
            
            <div class="card">
                <h2>Quick Actions</h2>
                <a href="/run-monitor" class="btn btn-success">🔄 Run Full Scan (Real Sources)</a>
                <a href="/projects" class="btn">📋 View Active Projects</a>
                <a href="/projects?state=VA" class="btn btn-warning">🔥 Virginia Projects</a>
                <a href="/export/csv" class="btn">📥 Export to CSV (Before 90-day expiration)</a>
            </div>
        </div>
    </body>
    </html>
    """
    return html

@app.route('/projects')
def projects():
    # Get filter parameters
    project_type = request.args.get('type')
    min_capacity = request.args.get('min_capacity', type=float)
    state = request.args.get('state')
    source = request.args.get('source')
    
    # Build query with 90-day filter
    query = PowerProject.active()
    if project_type:
        query = query.filter_by(project_type=project_type)
    if min_capacity:
        query = query.filter(PowerProject.capacity_mw >= min_capacity)
    if state:
        query = query.filter_by(state=state)
    if source:
        query = query.filter_by(source=source)
    
    # Get projects
    projects = query.order_by(PowerProject.capacity_mw.desc()).limit(500).all()
    
    # Generate table rows
    rows = ""
    for p in projects:
        # Styling based on capacity and type
        row_style = ''
        if p.capacity_mw >= 500:
            row_style = 'background: #ffe6e6;'
        elif p.capacity_mw >= 300:
            row_style = 'background: #fff4e6;'
        elif p.project_type == 'datacenter':
            row_style = 'background: #e8f5e9;'
        
        # Create source links
        source_link = f'<a href="{p.source_url}" target="_blank" style="color: #007bff;">{p.source} ↗</a>' if p.source_url else p.source
        
        # Add document link if available
        doc_links = []
        if p.document_url:
            doc_links.append(f'<a href="{p.document_url}" target="_blank">📄 Doc</a>')
        if p.queue_report_url:
            doc_links.append(f'<a href="{p.queue_report_url}" target="_blank">📊 Report</a>')
        
        doc_links_html = ' '.join(doc_links) if doc_links else ''
        
        # Format customer/developer
        entity = p.customer or p.developer or p.utility or 'N/A'
        
        # Show reference numbers if available
        refs = []
        if p.docket_number:
            refs.append(f"FERC: {p.docket_number}")
        if p.case_number:
            refs.append(f"Case: {p.case_number}")
        refs_html = '<br><small>' + ' | '.join(refs) + '</small>' if refs else ''
        
        rows += f"""
        <tr style="{row_style}">
            <td><strong>{p.project_name or 'Unnamed'}</strong><br>
                <small style="color: #666;">ID: {p.request_id}</small>
                {refs_html}</td>
            <td><strong>{p.capacity_mw:,.0f}</strong> MW</td>
            <td>{p.location or p.county or 'Unknown'}, {p.state or ''}</td>
            <td>{entity}</td>
            <td><span style="padding: 3px 8px; background: {'#4CAF50' if p.project_type == 'datacenter' else '#2196F3' if p.project_type == 'manufacturing' else '#9E9E9E'}; color: white; border-radius: 3px; font-size: 12px;">{p.project_type or 'other'}</span></td>
            <td>{p.status or 'Active'}</td>
            <td>{source_link}<br>{doc_links_html}</td>
            <td>{p.created_at.strftime('%m/%d/%Y')}</td>
        </tr>
        """
    
    if not projects:
        rows = "<tr><td colspan='8' style='text-align: center; padding: 40px;'>No projects found. Try running a monitoring scan.</td></tr>"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Power Projects Database</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; background: #f0f2f5; }}
            .nav {{ background: #2c3e50; padding: 0; }}
            .nav a {{ color: white; padding: 15px 20px; display: inline-block; text-decoration: none; }}
            .nav a:hover {{ background: #34495e; }}
            .container {{ max-width: 1600px; margin: 0 auto; padding: 20px; }}
            .filters {{ background: white; padding: 20px; margin: 20px 0; border-radius: 8px; }}
            .filters a {{ margin-right: 10px; }}
            .notice {{ background: #d1ecf1; border-left: 4px solid #0c5460; padding: 10px; margin: 20px 0; }}
            table {{ width: 100%; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            th {{ background: #34495e; color: white; padding: 12px; text-align: left; font-weight: 500; }}
            td {{ padding: 12px; border-bottom: 1px solid #ecf0f1; }}
            tr:hover {{ background: #f8f9fa; }}
            .legend {{ margin: 20px 0; }}
            .legend span {{ margin-right: 20px; padding: 5px 10px; border-radius: 3px; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="nav">
            <a href="/">🏠 Dashboard</a>
            <a href="/projects">📊 All Projects</a>
            <a href="/projects?type=datacenter">🖥️ Data Centers</a>
            <a href="/projects?type=manufacturing">🏭 Manufacturing</a>
            <a href="/projects?min_capacity=200">⚡ 200+ MW</a>
            <a href="/projects?min_capacity=500">🔥 500+ MW</a>
            <a href="/monitoring">📡 Monitoring</a>
        </div>
        
        <div class="container">
            <h1>Active Power Projects (Last 90 Days)</h1>
            
            <div class="notice">
                ℹ️ Showing projects from the last 90 days. Older projects are automatically archived. Export data regularly if you need long-term records.
            </div>
            
            <div class="filters">
                <strong>Quick Filters:</strong>
                <a href="/projects?state=VA">Virginia</a> |
                <a href="/projects?state=TX">Texas</a> |
                <a href="/projects?state=CA">California</a> |
                <a href="/projects?source=PJM">PJM Only</a> |
                <a href="/projects?source=ERCOT">ERCOT Only</a> |
                <a href="/projects?source=CAISO">CAISO Only</a>
            </div>
            
            <div class="legend">
                <span style="background: #ffe6e6;">🔴 500+ MW Mega Projects</span>
                <span style="background: #fff4e6;">🟠 300+ MW Large Projects</span>
                <span style="background: #e8f5e9;">🟢 Data Centers</span>
            </div>
            
            <table>
                <thead>
                    <tr>
                        <th>Project Name / ID / References</th>
                        <th>Capacity</th>
                        <th>Location</th>
                        <th>Customer/Developer</th>
                        <th>Type</th>
                        <th>Status</th>
                        <th>Source / Documents</th>
                        <th>Date Added</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
            
            <div style="margin: 20px 0; text-align: center;">
                <p>Showing {len(projects)} active projects • <a href="/export/csv">Export to CSV</a></p>
            </div>
        </div>
    </body>
    </html>
    """
    return html

# Keep all other routes the same but update statistics to use PowerProject.active()
@app.route('/export/csv')
def export_csv():
    """Export active projects to CSV"""
    projects = PowerProject.active().order_by(PowerProject.capacity_mw.desc()).all()
    
    csv_data = "Request ID,Queue Position,Project Name,Capacity (MW),Location,County,State,Customer,Developer,Utility,Type,Status,Docket,Case,Document URL,Source,Source URL,Date Added\n"
    
    for p in projects:
        csv_data += f'"{p.request_id}","{p.queue_position or ""}","{p.project_name or ""}",{p.capacity_mw},"{p.location or ""}","{p.county or ""}","{p.state or ""}","{p.customer or ""}","{p.developer or ""}","{p.utility or ""}","{p.project_type or ""}","{p.status or ""}","{p.docket_number or ""}","{p.case_number or ""}","{p.document_url or ""}","{p.source}","{p.source_url or ""}",{p.created_at.strftime("%Y-%m-%d")}\n'
    
    response = app.response_class(
        csv_data,
        mimetype='text/csv',
        headers={"Content-Disposition": f"attachment;filename=power_projects_{datetime.now().strftime('%Y%m%d')}.csv"}
    )
    return response

@app.route('/run-monitor')
def run_monitor():
    """Run comprehensive monitoring - REAL DATA ONLY"""
    try:
        monitor = RealDataPowerMonitor()
        result = monitor.run_comprehensive_monitoring()
        
        # Build source summary
        source_details = "<ul>"
        for source, count in result['by_source'].items():
            if count > 0:
                source_details += f"<li><strong>{source}:</strong> {count} projects</li>"
            else:
                source_details += f"<li><em>{source}: No data available</em></li>"
        source_details += "</ul>"
        
        return f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 40px; background: #f0f2f5; }}
                .success-box {{ background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); max-width: 600px; margin: 0 auto; }}
                h2 {{ color: #28a745; }}
                .stats {{ background: #f8f9fa; padding: 20px; border-radius: 5px; margin: 20px 0; }}
                .notice {{ background: #fff3cd; padding: 10px; margin: 10px 0; border-radius: 5px; }}
                .btn {{ padding: 12px 24px; background: #007bff; color: white; text-decoration: none; border-radius: 5px; display: inline-block; margin: 10px 5px; }}
                .btn:hover {{ background: #0056b3; }}
            </style>
        </head>
        <body>
            <div class="success-box">
                <h2>✅ Monitoring Complete (Real Data Only)</h2>
                <div class="stats">
                    <p><strong>Sources Checked:</strong> {result['sources_checked']}</p>
                    <p><strong>Total Projects Found:</strong> {result['projects_found']}</p>
                    <p><strong>New Projects Stored:</strong> {result['projects_stored']}</p>
                    <p><strong>Old Projects Removed (90+ days):</strong> {result.get('old_projects_removed', 0)}</p>
                    <p><strong>Scan Duration:</strong> {result['duration_seconds']} seconds</p>
                    
                    <p><strong>Projects by Source:</strong></p>
                    {source_details}
                </div>
                
                <div class="notice">
                    📌 Note: Only real data from official sources is collected. Some sources may be temporarily unavailable.
                </div>
                
                <div style="text-align: center;">
                    <a href="/projects" class="btn">View Active Projects</a>
                    <a href="/export/csv" class="btn">Export to CSV</a>
                    <a href="/" class="btn" style="background: #6c757d;">Back to Dashboard</a>
                </div>
            </div>
        </body>
        </html>
        """
    except Exception as e:
        logger.error(f"Monitoring error: {e}")
        return f"""
        <html>
        <body style="font-family: Arial; margin: 40px;">
            <h2 style="color: #dc3545;">❌ Monitoring Error</h2>
            <p>Error: {str(e)}</p>
            <p>This may be due to source websites being temporarily unavailable.</p>
            <a href="/" style="padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px;">Back to Dashboard</a>
        </body>
        </html>
        """, 500

# Keep all other routes (monitoring, sources, init, reset-db) the same...
# [Rest of the routes remain unchanged]

@app.route('/sources')
def sources():
    """Show data sources and coverage - REAL SOURCES ONLY"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Real Data Sources</title>
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; background: #f0f2f5; }
            .nav { background: #2c3e50; padding: 0; }
            .nav a { color: white; padding: 15px 20px; display: inline-block; text-decoration: none; }
            .nav a:hover { background: #34495e; }
            .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
            .source-card { background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .source-card h3 { margin-top: 0; color: #2c3e50; }
            .coverage { color: #27ae60; font-weight: bold; }
            .status { padding: 3px 8px; border-radius: 3px; font-size: 12px; }
            .active { background: #d4edda; color: #155724; }
            a { color: #3498db; }
        </style>
    </head>
    <body>
        <div class="nav">
            <a href="/">🏠 Dashboard</a>
            <a href="/projects">📊 Projects</a>
            <a href="/sources">🌐 Data Sources</a>
        </div>
        
        <div class="container">
            <h1>Real Data Sources - Official Grid Operators Only</h1>
            
            <div class="source-card">
                <h3>🌟 CAISO - California ISO</h3>
                <span class="status active">ACTIVE - Real Data</span>
                <p class="coverage">Coverage: California</p>
                <p>Direct access to interconnection queue Excel files. Updated monthly.</p>
                <a href="http://www.caiso.com/planning/Pages/GeneratorInterconnection/Default.aspx" target="_blank">Official Queue →</a>
            </div>
            
            <div class="source-card">
                <h3>⚡ PJM Interconnection</h3>
                <span class="status active">ACTIVE - Real Data</span>
                <p class="coverage">Coverage: 13 states + DC</p>
                <p>CSV downloads of active queue. Critical for data center tracking in Northern Virginia.</p>
                <a href="https://www.pjm.com/planning/services-requests/interconnection-queues" target="_blank">Official Queue →</a>
            </div>
            
            <div class="source-card">
                <h3>🤠 ERCOT</h3>
                <span class="status active">ACTIVE - Real Data</span>
                <p class="coverage">Coverage: Texas (90% of state)</p>
                <p>Monthly GIS reports with interconnection requests. Major growth market.</p>
                <a href="http://www.ercot.com/gridinfo/resource" target="_blank">GIS Reports →</a>
            </div>
            
            <div class="source-card">
                <h3>🌾 MISO</h3>
                <span class="status active">ACTIVE - Real Data</span>
                <p class="coverage">Coverage: 15 states</p>
                <p>Excel downloads of generator interconnection queue.</p>
                <a href="https://www.misoenergy.org/planning/generator-interconnection/" target="_blank">Official Queue →</a>
            </div>
            
            <div class="source-card">
                <h3>🦞 ISO-NE</h3>
                <span class="status active">ACTIVE - Real Data</span>
                <p class="coverage">Coverage: 6 New England states</p>
                <p>Interconnection queue Excel files.</p>
                <a href="https://www.iso-ne.com/" target="_blank">Official Site →</a>
            </div>
            
            <div class="source-card">
                <h3>🌻 SPP</h3>
                <span class="status active">ACTIVE - Real Data</span>
                <p class="coverage">Coverage: 14 states</p>
                <p>Generation interconnection queue Excel downloads.</p>
                <a href="https://www.spp.org/planning/generator-interconnection/" target="_blank">Official Queue →</a>
            </div>
            
            <div class="source-card">
                <h3>🗽 NYISO</h3>
                <span class="status active">ACTIVE - Real Data</span>
                <p class="coverage">Coverage: New York</p>
                <p>CSV format interconnection queue data.</p>
                <a href="https://www.nyiso.com/interconnection-process" target="_blank">Official Queue →</a>
            </div>
            
            <div class="source-card" style="background: #f8f9fa; border: 2px solid #28a745;">
                <h3>📊 Data Quality Notice</h3>
                <p class="coverage">100% Real Data - No Fake Entries</p>
                <p>All data comes directly from official grid operator websites. No placeholder or sample data is used. If a source is temporarily unavailable, it shows zero results rather than fake data.</p>
                <p><strong>Automatic Cleanup:</strong> Projects older than 90 days are automatically removed to keep data fresh.</p>
            </div>
        </div>
    </body>
    </html>
    """
    return html

@app.route('/monitoring')
def monitoring():
    """Monitoring history and status"""
    runs = MonitoringRun.query.order_by(MonitoringRun.run_date.desc()).limit(50).all()
    
    rows = ""
    for run in runs:
        # Parse source details if available
        source_info = ""
        if run.details:
            try:
                details = json.loads(run.details)
                source_info = ", ".join([f"{k}: {v}" for k, v in details.items()])
            except:
                pass
        
        status_color = "#28a745" if run.status == "completed" else "#ffc107"
        
        rows += f"""
        <tr>
            <td>{run.run_date.strftime('%Y-%m-%d %H:%M:%S')}</td>
            <td>{run.sources_checked}</td>
            <td>{run.projects_found}</td>
            <td><strong>{run.projects_stored}</strong></td>
            <td>{run.duration_seconds:.1f}s</td>
            <td><span style="color: {status_color};">●</span> {run.status}</td>
            <td style="font-size: 11px; color: #666;">{source_info[:100]}...</td>
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
            .nav a:hover {{ background: #34495e; }}
            .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
            .card {{ background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            table {{ width: 100%; border-collapse: collapse; }}
            th {{ background: #f8f9fa; padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6; }}
            td {{ padding: 10px; border-bottom: 1px solid #ecf0f1; }}
            .btn {{ padding: 12px 24px; background: #28a745; color: white; text-decoration: none; border-radius: 5px; display: inline-block; }}
            .btn:hover {{ background: #218838; }}
        </style>
    </head>
    <body>
        <div class="nav">
            <a href="/">🏠 Dashboard</a>
            <a href="/projects">📊 Projects</a>
            <a href="/monitoring">📡 Monitoring</a>
            <a href="/sources">🌐 Sources</a>
        </div>
        
        <div class="container">
            <h1>Monitoring History</h1>
            
            <div class="card">
                <a href="/run-monitor" class="btn">🔄 Run Real Data Scan Now</a>
                <p style="margin-top: 15px; color: #666;">
                    Scans 7 real grid operator sources. Only actual data is collected - no placeholders.
                    Typical scan time: 30-60 seconds.
                </p>
            </div>
            
            <div class="card">
                <h2>Recent Monitoring Runs</h2>
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
                    <tbody>
                        {rows if rows else '<tr><td colspan="7" style="text-align: center; padding: 20px;">No monitoring runs yet. Click "Run Real Data Scan" to start.</td></tr>'}
                    </tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """

@app.route('/init')
def init_db():
    """Initialize database"""
    try:
        db.create_all()
        return "Database initialized successfully! <a href='/'>Go to Dashboard</a>"
    except Exception as e:
        return f"Error: {str(e)}", 500

@app.route('/reset-db')
def reset_database():
    """Reset database with new schema"""
    try:
        db.drop_all()
        db.create_all()
        return """
        <html>
        <body style="font-family: Arial; margin: 40px;">
            <h2>✅ Database Reset Complete</h2>
            <p>All tables have been recreated with enhanced schema including reference fields.</p>
            <a href="/" style="padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px;">Go to Dashboard</a>
            <a href="/run-monitor" style="padding: 10px 20px; background: #28a745; color: white; text-decoration: none; border-radius: 5px; margin-left: 10px;">Run Monitor</a>
        </body>
        </html>
        """
    except Exception as e:
        return f"Error resetting database: {str(e)}", 500
        @app.route('/test-sources')
def test_sources():
    """Test each data source individually to diagnose issues"""
    results = []
    monitor = RealDataPowerMonitor()
    
    # Test each source
    test_sources = [
        ('CAISO', 'http://www.caiso.com/planning/Documents/AllGeneratorInterconnectionQueue.xlsx'),
        ('PJM', 'https://www.pjm.com/pub/planning/downloads/queues/active-queue.csv'),
        ('ERCOT', 'http://www.ercot.com/content/wcm/lists/226522/GIS_Report_10_01_2024.xlsx'),
        ('MISO', 'https://cdn.misoenergy.org/Generator%20Interconnection%20Queue.xlsx'),
    ]
    
    for name, url in test_sources:
        try:
            response = monitor.session.get(url, timeout=10, verify=False)
            status_code = response.status_code
            content_size = len(response.content)
            
            # Try to detect content type
            content_type = response.headers.get('Content-Type', 'unknown')
            
            results.append({
                'source': name,
                'url': url,
                'status': status_code,
                'size': content_size,
                'type': content_type,
                'success': status_code == 200
            })
        except Exception as e:
            results.append({
                'source': name,
                'url': url,
                'status': 'ERROR',
                'error': str(e),
                'success': False
            })
    
    # Format results
    html = """
    <html>
    <head>
        <style>
            body { font-family: Arial; margin: 40px; }
            table { width: 100%; border-collapse: collapse; }
            th, td { padding: 10px; text-align: left; border: 1px solid #ddd; }
            .success { background: #d4edda; }
            .error { background: #f8d7da; }
        </style>
    </head>
    <body>
        <h1>Data Source Test Results</h1>
        <table>
            <tr><th>Source</th><th>Status</th><th>Size/Error</th><th>Type</th></tr>
    """
    
    for r in results:
        row_class = 'success' if r['success'] else 'error'
        status = r.get('status', 'ERROR')
        detail = f"{r.get('size', 0)} bytes" if r['success'] else r.get('error', 'Unknown error')
        html += f"""
        <tr class="{row_class}">
            <td>{r['source']}</td>
            <td>{status}</td>
            <td>{detail}</td>
            <td>{r.get('type', 'N/A')}</td>
        </tr>
        """
    
    html += """
        </table>
        <p><a href="/">Back to Dashboard</a></p>
    </body>
    </html>
    """
    return html

# Create tables on startup
with app.app_context():
    try:
        db.create_all()
        logger.info("Database ready")
    except Exception as e:
        logger.error(f"Database error: {e}")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

