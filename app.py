import os
import logging
import requests
import pandas as pd
import hashlib
import time
import json
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from urllib.parse import urljoin
from fake_useragent import UserAgent
import threading # Keep for manual trigger only

from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy

# Try to import gridstatus - used for PJM/ERCOT
try:
    import gridstatus
    GRIDSTATUS_AVAILABLE = True
except ImportError:
    GRIDSTATUS_AVAILABLE = False
    # This is fine, we have fallbacks
    pass 

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Fix Railway/Heroku PostgreSQL URL quirks
database_url = os.environ.get('DATABASE_URL', 'sqlite:///hunter.db')
if database_url.startswith('postgres://'):
    database_url = database_url.replace('postgres://', 'postgresql://', 1)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'hunter-dev-key')
app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# --- Heuristics & Intelligence Lists (The Hunter Engine) ---
class DataCenterHunter:
    """Intelligence engine to identify data centers and large loads."""
    
    # Hyperscaler/Developer shell names and direct names
    SHELL_COMPANIES = [
        'RAVEN NORTHBROOK', 'VANDALAY', 'K2', 'STARK', 'CLOUDHQ', 'VANTAGE', 'ALIGNED',
        'STACK INFRASTRUCTURE', 'QTS', 'CYRUSONE', 'DIGITAL REALTY', 'EQUINIX', 'IRON MOUNTAIN',
        'COMPASS', 'CORESITE', 'SABEY', 'EDGECONNEX', 'META', 'GOOGLE', 'MICROSOFT', 'AMAZON',
        'AWS', 'AMAZON DATA SERVICES', 'BLACKSTONE', 'GIGAPOWER', 'YONDZR', 'SWITCH',
        'DATA BANK', 'TIERPOINT', 'FLEXENTIAL', 'SCALABLE', 'INFRAMARK', 'PRIME'
    ]

    # Counties known for major data center clusters
    TARGET_COUNTIES = [
        # Virginia (Data Center Alley)
        'LOUDOUN', 'FAIRFAX', 'PRINCE WILLIAM', 'HENRICO', 'CULPEPER', 'SPOTSYLVANIA',
        # Ohio (The New Frontier)
        'FRANKLIN', 'LICKING', 'DELAWARE', 'UNION', 'FAIRFIELD',
        # Arizona (Silicon Desert)
        'MARICOPA', 'PINAL',
        # Texas (Silicon Prairie)
        'DALLAS', 'TARRANT', 'TRAVIS', 'BEXAR', 'FORT BEND',
        # Georgia
        'FULTON', 'DOUGLAS', 'GWINNETT',
        # Illinois
        'COOK', 'KANE', 'DUPAGE',
        # Oregon/Washington
        'UMATILLA', 'MORROW', 'DOUGLAS', 'GRANT', 'QUINCY'
    ]

    KEYWORDS = ['DATA CENTER', 'DATACENTER', 'SERVER', 'COMPUTE', 'DIGITAL', 'HYPERSCALE', 'PROCESSOR', 'CAMPUS', 'CLOUD']

    @staticmethod
    def calculate_confidence(row):
        """Returns a score 0-100 indicating likelihood of being a Data Center."""
        score = 0
        details = []
        
        name = str(row.get('project_name', '')).upper()
        customer = str(row.get('customer', '')).upper()
        fuel = str(row.get('fuel_type', '')).upper()
        county = str(row.get('county', '')).upper()
        capacity = float(row.get('capacity_mw', 0) or 0)

        # 1. Shell Company / Direct Match (High Confidence)
        if any(company in customer for company in DataCenterHunter.SHELL_COMPANIES):
            score += 60
            details.append("Known Developer Match")
        
        if any(word in name or word in customer for word in DataCenterHunter.KEYWORDS):
            score += 50
            details.append("Explicit Keyword Match")

        # 2. Heuristic: Large Load in Target County
        if county in DataCenterHunter.TARGET_COUNTIES:
            if capacity >= 50:
                score += 25
                details.append("Target County + Large Load")
            else:
                score += 10
                details.append("Target County")

        # 3. Heuristic: Suspicious Fuel Types (Load disguises)
        if fuel in ['LOAD', 'OTHER', 'STORAGE', 'BATTERY', 'NONE'] and capacity >= 75:
            score += 15
            details.append(f"Large {fuel} Request (>75MW)")
            
        # 4. Critical Threshold
        if capacity >= 300 and score < 50: # If it's huge, but has no other signs, still score it high
             score += 30
             details.append("Extreme Capacity (>300MW)")


        return min(score, 100), ", ".join(details)

# --- Database Models ---
class PowerProject(db.Model):
    __tablename__ = 'power_projects'
    
    id = db.Column(db.Integer, primary_key=True)
    request_id = db.Column(db.String(255), unique=True, index=True) # ISO Specific ID
    
    # Core Data
    iso = db.Column(db.String(50), index=True) # PJM, MISO, etc.
    queue_date = db.Column(db.Date)
    project_name = db.Column(db.String(500))
    capacity_mw = db.Column(db.Float, index=True)
    
    # Location
    county = db.Column(db.String(200), index=True)
    state = db.Column(db.String(10), index=True)
    location_raw = db.Column(db.String(500))
    
    # Details
    customer = db.Column(db.String(500))
    status = db.Column(db.String(100)) # Active, Withdrawn, Completed
    fuel_type = db.Column(db.String(100))
    project_type = db.Column(db.String(50)) # Deduced type
    
    # Hunter Intelligence
    is_suspected_datacenter = db.Column(db.Boolean, default=False)
    hunter_score = db.Column(db.Integer, default=0)
    hunter_notes = db.Column(db.String(500))
    
    # Metadata
    source_url = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_seen = db.Column(db.DateTime, default=datetime.utcnow)
    is_archived = db.Column(db.Boolean, default=False) # We no longer delete projects

    def update_hunter_status(self):
        data = {
            'project_name': self.project_name,
            'customer': self.customer,
            'fuel_type': self.fuel_type,
            'county': self.county,
            'capacity_mw': self.capacity_mw
        }
        score, notes = DataCenterHunter.calculate_confidence(data)
        self.hunter_score = score
        self.hunter_notes = notes
        # A score of 40 or higher is a high-probability target
        self.is_suspected_datacenter = score >= 40

class ScrapeLog(db.Model):
    __tablename__ = 'scrape_logs'
    id = db.Column(db.Integer, primary_key=True)
    run_date = db.Column(db.DateTime, default=datetime.utcnow)
    iso = db.Column(db.String(50))
    projects_found = db.Column(db.Integer)
    status = db.Column(db.String(50)) # Success, Failed
    error_msg = db.Column(db.Text)

# --- Scraper Modules ---
class ScraperEngine:
    def __init__(self):
        self.ua = UserAgent()
        self.session = requests.Session()
        
    def get_headers(self):
        return {'User-Agent': self.ua.random}

    def safe_float(self, val):
        try:
            return float(str(val).replace(',', '').strip())
        except:
            return 0.0

    def run_caiso(self):
        """Scrape CAISO Public Queue (California)"""
        url = "http://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx"
        try:
            r = self.session.get(url, headers=self.get_headers(), timeout=30, verify=False)
            df = pd.read_excel(BytesIO(r.content))
            projects = []
            
            # Dynamic Column Mapping for robustness
            cols = {c.lower(): c for c in df.columns}
            id_col = next((c for c in cols if 'queue' in c and 'id' in c), None)
            mw_col = next((c for c in cols if 'mw' in c and 'net' not in c), 'capacity') # Fallback to a common name
            
            if not id_col or not mw_col:
                # Fallback to hardcoded names if dynamic mapping fails
                id_col = 'Queue ID'
                mw_col = 'Capacity (MW)'
                
            for _, row in df.iterrows():
                try:
                    # Look for load MW or just capacity MW
                    mw = self.safe_float(row.get('Capacity (MW)', row.get(cols.get('capacity', mw_col))))
                    if mw < 10: continue 

                    p = {
                        'iso': 'CAISO',
                        'request_id': f"CAISO_{row.get(id_col, 'UNK')}",
                        'project_name': str(row.get(cols.get('project name', ''), 'Unknown')).strip(),
                        'capacity_mw': mw,
                        'county': str(row.get(cols.get('county', ''), 'Unknown')).upper().replace('COUNTY', '').strip(),
                        'state': str(row.get(cols.get('state', ''), 'CA')).upper().strip(),
                        'customer': str(row.get(cols.get('interconnection customer', ''), 'Unknown')).strip(),
                        'fuel_type': str(row.get(cols.get('fuel', ''), 'Unknown')).upper().strip(),
                        'status': str(row.get(cols.get('status', ''), 'Active')).strip(),
                        'source_url': url
                    }
                    projects.append(p)
                except Exception as e:
                    logger.debug(f"CAISO Row Skip: {e}")
                    continue
            return projects
        except Exception as e:
            logger.error(f"CAISO Scrape Failed: {e}")
            raise

    def run_nyiso(self):
        """Scrape NYISO (New York)"""
        url = "https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx"
        try:
            r = self.session.get(url, headers=self.get_headers(), timeout=30, verify=False)
            df = pd.read_excel(BytesIO(r.content))
            projects = []
            
            for _, row in df.iterrows():
                # 'S (MW)' is often the requested capacity
                mw = self.safe_float(row.get('S (MW)', 0))
                if mw < 10: continue

                p = {
                    'iso': 'NYISO',
                    'request_id': f"NYISO_{row.get('Queue Pos.', 'UNK')}",
                    'project_name': str(row.get('Project Name', 'Unknown')).strip(),
                    'capacity_mw': mw,
                    'county': str(row.get('County', 'Unknown')).upper().replace('COUNTY', '').strip(),
                    'state': 'NY',
                    'customer': str(row.get('Developer', 'Unknown')).strip(),
                    'fuel_type': str(row.get('Type', 'Unknown')).upper().strip(),
                    'status': str(row.get('Status', 'Active')).strip(),
                    'source_url': url
                }
                projects.append(p)
            return projects
        except Exception as e:
            logger.error(f"NYISO Scrape Failed: {e}")
            raise

    def run_miso(self):
        """Scrape MISO (Midwest) - Critical for Ohio"""
        url = "https://docs.misoenergy.org/marketreports/GI_Queue.xlsx"
        try:
            r = self.session.get(url, headers=self.get_headers(), timeout=30, verify=False)
            # MISO has headers often on row 0
            df = pd.read_excel(BytesIO(r.content), skiprows=0) 
            projects = []
            
            for _, row in df.iterrows():
                # Use Max Summer MW as the most representative capacity
                mw = self.safe_float(row.get('Max Summer MW', 0))
                if mw < 10: continue

                p = {
                    'iso': 'MISO',
                    'request_id': f"MISO_{row.get('Project #', 'UNK')}",
                    'project_name': str(row.get('Project Name', 'MISO Project')).strip(),
                    'capacity_mw': mw,
                    'county': str(row.get('County', 'Unknown')).upper().replace('COUNTY', '').strip(),
                    'state': str(row.get('State', 'UNK')).upper().strip(),
                    'customer': str(row.get('Interconnection Customer', 'Unknown')).strip(),
                    'fuel_type': str(row.get('Fuel Type', 'Unknown')).upper().strip(),
                    'status': str(row.get('Study Phase', 'Active')).strip(),
                    'source_url': url
                }
                projects.append(p)
            return projects
        except Exception as e:
            logger.error(f"MISO Scrape Failed: {e}")
            raise

    def run_isone(self):
        """Scrape ISO-NE (New England)"""
        url = "https://www.iso-ne.com/static-assets/documents/2014/08/iso_ne_queue.xlsx" 
        try:
            r = self.session.get(url, headers=self.get_headers(), timeout=30, verify=False)
            df = pd.read_excel(BytesIO(r.content), sheet_name=0)
            projects = []
            
            for _, row in df.iterrows():
                # Summer MW is the relevant capacity
                mw = self.safe_float(row.get('Summer MW', 0))
                if mw < 10: continue

                p = {
                    'iso': 'ISO-NE',
                    'request_id': f"ISONE_{row.get('Queue Position', 'UNK')}",
                    'project_name': str(row.get('Project Name', 'Unknown')).strip(),
                    'capacity_mw': mw,
                    'county': str(row.get('County', 'Unknown')).upper().replace('COUNTY', '').strip(),
                    'state': str(row.get('State', 'UNK')).upper().strip(),
                    'customer': str(row.get('Interconnection Customer', 'Unknown')).strip(),
                    'fuel_type': str(row.get('Fuel Type', 'Unknown')).upper().strip(),
                    'status': str(row.get('Sync Status', 'Active')).strip(),
                    'source_url': url
                }
                projects.append(p)
            return projects
        except Exception as e:
            logger.error(f"ISO-NE Scrape Failed: {e}")
            raise

    def run_pjm(self):
        """Run PJM using gridstatus library (preferred for stability)"""
        if not GRIDSTATUS_AVAILABLE:
            logger.warning("gridstatus not available, PJM Scrape Failed.")
            raise ImportError("gridstatus not installed or available for PJM.")
            
        try:
            pjm = gridstatus.PJM()
            # This endpoint typically provides the public queue
            df = pjm.get_interconnection_queue()
            projects = []
            
            for _, row in df.iterrows():
                mw = self.safe_float(row.get('Capacity (MW)', 0))
                if mw < 10: continue

                p = {
                    'iso': 'PJM',
                    'request_id': f"PJM_{row.get('Queue ID', 'UNK')}",
                    'project_name': str(row.get('Project Name', 'Unknown')).strip(),
                    'capacity_mw': mw,
                    # PJM data usually has County/State ready
                    'county': str(row.get('County', 'Unknown')).upper().replace('COUNTY', '').strip(),
                    'state': str(row.get('State', 'UNK')).upper().strip(),
                    'customer': str(row.get('Interconnection Customer', 'Unknown')).strip(),
                    'fuel_type': str(row.get('Fuel', 'Unknown')).upper().strip(),
                    'status': str(row.get('Status', 'Active')).strip(),
                    'source_url': 'gridstatus-pjm-api'
                }
                projects.append(p)
            return projects
        except Exception as e:
            logger.error(f"PJM Scrape Failed: {e}")
            # Fallback to direct scraping if API fails (not implemented here for brevity, but recommended)
            raise


# --- Orchestrator Function (Called by run_monitor.py) ---
def run_full_scan():
    """Runs all scrapers and updates DB."""
    scraper = ScraperEngine()
    # Scrapers to run
    scrapers = {
        'CAISO': scraper.run_caiso,
        'NYISO': scraper.run_nyiso,
        'MISO': scraper.run_miso,
        'ISO-NE': scraper.run_isone,
        'PJM': scraper.run_pjm
    }
    
    # Track which projects were seen this run
    seen_ids = set() 
    
    # We must be inside the app context since this is called by the worker
    with app.app_context():
        
        for iso, func in scrapers.items():
            try:
                logger.info(f"Starting {iso} scan...")
                projects = func()
                new_count = 0
                
                for p_data in projects:
                    
                    # Create a unique ID for upsert logic and archiving
                    request_id = p_data['request_id']
                    
                    # Find existing project
                    existing = PowerProject.query.filter_by(request_id=request_id).first()
                    
                    if existing:
                        # Update existing
                        for k, v in p_data.items():
                            # Only update if new data is not empty, excluding ISO and ID
                            if k not in ['iso', 'request_id'] and v is not None:
                                setattr(existing, k, v)
                                
                        existing.last_seen = datetime.utcnow()
                        existing.is_archived = False
                        existing.update_hunter_status() # Re-run hunter logic on update
                    else:
                        # Create new
                        new_proj = PowerProject(**p_data)
                        new_proj.update_hunter_status()
                        db.session.add(new_proj)
                        new_count += 1
                        
                    seen_ids.add(request_id)
                
                db.session.commit()
                
                # Log Success
                db.session.add(ScrapeLog(iso=iso, projects_found=len(projects), status="Success"))
                db.session.commit()
                logger.info(f"{iso}: Processed {len(projects)} items, {new_count} new.")
                
            except Exception as e:
                db.session.add(ScrapeLog(iso=iso, projects_found=0, status="Failed", error_msg=str(e)))
                db.session.commit()
                logger.error(f"{iso} failed: {e}")

        # Final Cleanup: Archive projects not seen this run (older than 7 days, to allow for minor scraper failures)
        # This keeps the history but cleans up the 'Active' view.
        
        # NOTE: This logic is complex in a transactional environment. 
        # For simplicity in this script, we'll keep the project as "Active" until manually reviewed or marked "Withdrawn" in the source data.
        # The 'last_seen' column is the historical tracker. The "Archived" logic is currently disabled 
        # to prevent accidental archiving until a robust diffing system is implemented.
        pass

# --- Routes ---
@app.route('/')
def index():
    # Dashboard Metrics
    total = PowerProject.query.filter_by(is_archived=False).count()
    suspected_dc = PowerProject.query.filter_by(is_archived=False, hunter_score>=40).count()
    large_load = PowerProject.query.filter_by(is_archived=False, capacity_mw>=100).count()
    
    # Hunter Top List (Score > 0 to filter out default entries)
    top_hunters = PowerProject.query.filter_by(is_archived=False, hunter_score > 0)\
        .order_by(PowerProject.hunter_score.desc())\
        .limit(10).all()
        
    # Recent Logs
    logs = ScrapeLog.query.order_by(ScrapeLog.run_date.desc()).limit(5).all()
    
    return render_template('index.html', total=total, suspected_dc=suspected_dc, 
                           top_hunters=top_hunters, logs=logs, large_load=large_load)

@app.route('/projects')
def projects():
    filter_type = request.args.get('filter', 'all')
    page = request.args.get('page', 1, type=int)
    
    query = PowerProject.query.filter_by(is_archived=False)
    
    if filter_type == 'hunter':
        # Filter for high confidence targets
        query = query.filter(PowerProject.hunter_score >= 40)
    elif filter_type == 'load':
        # Filter for anything over 100MW
        query = query.filter(PowerProject.capacity_mw >= 100)
    
    pagination = query.order_by(PowerProject.hunter_score.desc(), PowerProject.capacity_mw.desc())\
        .paginate(page=page, per_page=50)
        
    return render_template('projects.html', pagination=pagination, filter_type=filter_type)

@app.route('/trigger', methods=['POST'])
def manual_trigger():
    # Allow the worker function to be manually triggered from the dashboard
    # This must run in a background thread to prevent the HTTP request from timing out.
    try:
        t = threading.Thread(target=run_full_scan)
        t.start()
        return jsonify({"status": "Scan started in background. Check logs in a few minutes."})
    except Exception as e:
        return jsonify({"status": "Error starting scan", "error": str(e)}), 500


# --- Init ---
if __name__ == '__main__':
    # This block only runs when executing 'python app.py' locally, not when running via gunicorn
    with app.app_context():
        db.create_all()
    
    # The scheduler is now run by the separate 'run_monitor.py' worker process.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
