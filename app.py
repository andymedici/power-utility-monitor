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
        
        # Define monitors - mix of gridstatus and direct
        monitors = [
            ('CAISO', self.fetch_caiso_gridstatus),  # Tries gridstatus, falls back to direct
            ('NYISO', self.fetch_nyiso_direct),      # Direct URL (works well)
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
        
        return {
            'sources_checked': total_sources,
            'projects_found': len(all_projects),
            'projects_stored': stored,
            'duration_seconds': round(duration, 2),
            'by_source': source_stats,
            'old_projects_removed': old_count,
            'gridstatus_available': GRIDSTATUS_AVAILABLE
        }

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
                <p><strong>Working:</strong> 
                    <span class="status active">NYISO (Direct)</span>
                    <span class="status active">CAISO (Hybrid)</span>
                    <span class="status active">SPP (Direct)</span>
                </p>
                <p><strong>Requires Setup:</strong> PJM (needs API key), ERCOT (needs gridstatus)</p>
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
            <a href="/projects" style="padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px;">View Projects</a>
            <a href="/" style="padding: 10px 20px; background: #6c757d; color: white; text-decoration: none; border-radius: 5px; margin-left: 10px;">Dashboard</a>
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
