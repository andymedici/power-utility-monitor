# app.py - Complete Power Utility Monitor in One File
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash, make_response
from flask_sqlalchemy import SQLAlchemy
import os
import requests
import pandas as pd
import re
import json
import hashlib
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import io
from urllib.parse import urljoin
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import csv

# Flask App Setup
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///power_monitor.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize extensions
db = SQLAlchemy(app)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE MODELS
# ============================================================================

class PowerRequest(db.Model):
    __tablename__ = 'power_requests'
    
    id = db.Column(db.Integer, primary_key=True)
    request_id = db.Column(db.String(255), unique=True, nullable=False, index=True)
    project_name = db.Column(db.Text)
    capacity_mw = db.Column(db.Float, nullable=False, index=True)
    location = db.Column(db.Text)
    state = db.Column(db.String(2), index=True)
    utility = db.Column(db.String(255))
    customer = db.Column(db.Text)
    request_date = db.Column(db.Date, index=True)
    status = db.Column(db.String(100))
    project_type = db.Column(db.String(50), index=True)
    source = db.Column(db.String(100), nullable=False, index=True)
    source_url = db.Column(db.Text)
    confidence_score = db.Column(db.Integer, default=5)
    data_hash = db.Column(db.String(32), unique=True)
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self):
        return {
            'id': self.id,
            'request_id': self.request_id,
            'project_name': self.project_name,
            'capacity_mw': self.capacity_mw,
            'location': self.location,
            'state': self.state,
            'utility': self.utility,
            'customer': self.customer,
            'request_date': self.request_date.isoformat() if self.request_date else None,
            'status': self.status,
            'project_type': self.project_type,
            'source': self.source,
            'confidence_score': self.confidence_score,
            'latitude': self.latitude,
            'longitude': self.longitude,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }

class MonitoringRun(db.Model):
    __tablename__ = 'monitoring_runs'
    
    id = db.Column(db.Integer, primary_key=True)
    run_date = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    sources_checked = db.Column(db.Integer)
    requests_found = db.Column(db.Integer)
    requests_stored = db.Column(db.Integer)
    duration_seconds = db.Column(db.Float)
    status = db.Column(db.String(50))
    error_message = db.Column(db.Text)

class AlertSubscription(db.Model):
    __tablename__ = 'alert_subscriptions'
    
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), nullable=False, index=True)
    min_capacity = db.Column(db.Float, default=200)
    states = db.Column(db.Text)  # JSON array
    project_types = db.Column(db.Text)  # JSON array
    active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# ============================================================================
# DATA COLLECTION CLASS
# ============================================================================

class PowerUtilityMonitor:
    def __init__(self):
        self.min_capacity_mw = 100
        self.geocoder = Nominatim(user_agent="PowerUtilityMonitor/1.0")
        
        # Keywords for project classification
        self.datacenter_keywords = [
            'data center', 'datacenter', 'server farm', 'cloud computing',
            'artificial intelligence', 'ai', 'machine learning', 'hyperscale',
            'colocation', 'colo', 'computing facility', 'nvidia', 'gpu',
            'facebook', 'meta', 'google', 'amazon', 'microsoft', 'apple',
            'aws', 'azure', 'gcp', 'edge computing'
        ]
        
        self.manufacturing_keywords = [
            'manufacturing', 'factory', 'plant', 'production facility',
            'semiconductor', 'chip', 'fab', 'foundry', 'assembly',
            'automotive', 'battery', 'steel', 'aluminum', 'chemical'
        ]

    def extract_capacity(self, text: str) -> Optional[float]:
        """Extract capacity in MW from text"""
        if not text:
            return None
            
        patterns = [
            r'(\d+(?:\.\d+)?)\s*MW',
            r'(\d+(?:\.\d+)?)\s*megawatt',
            r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*kW',
            r'capacity[:\s]*(\d+(?:\.\d+)?)\s*MW'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                try:
                    capacity = float(matches[0].replace(',', ''))
                    if 'kW' in pattern:
                        capacity = capacity / 1000
                    if capacity >= self.min_capacity_mw:
                        return capacity
                except ValueError:
                    continue
        
        return None

    def classify_project_type(self, project_name: str, customer: str) -> str:
        """Classify project type based on keywords"""
        text = (project_name + " " + customer).lower()
        
        if any(keyword in text for keyword in self.datacenter_keywords):
            return 'datacenter'
        
        if any(keyword in text for keyword in self.manufacturing_keywords):
            return 'manufacturing'
        
        if any(keyword in text for keyword in ['battery', 'storage', 'bess']):
            return 'energy_storage'
        
        return 'unknown'

    def geocode_location(self, location: str, state: str = None) -> tuple:
        """Geocode location using Nominatim"""
        if not location:
            return None, None
            
        try:
            time.sleep(1.1)  # Respect rate limit
            search_query = f"{location}, {state}, USA" if state else f"{location}, USA"
            result = self.geocoder.geocode(search_query)
            
            if result:
                return result.latitude, result.longitude
                
        except (GeocoderTimedOut, Exception) as e:
            logger.warning(f"Geocoding failed for {location}: {e}")
        
        return None, None

    def generate_data_hash(self, data: dict) -> str:
        """Generate hash for duplicate detection"""
        key_data = f"{data.get('project_name', '').lower()}_{data.get('capacity_mw', 0)}_{data.get('location', '').lower()}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def monitor_caiso(self) -> List[Dict]:
        """Monitor CAISO queue"""
        logger.info("Monitoring CAISO...")
        requests_found = []
        
        try:
            response = requests.get('http://www.caiso.com/Documents/AllGeneratorInterconnectionQueue.xls', timeout=60)
            
            if response.status_code == 200:
                df = pd.read_excel(io.BytesIO(response.content), engine='openpyxl')
                df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
                
                for _, row in df.iterrows():
                    try:
                        # Find capacity
                        capacity_cols = [col for col in df.columns if 'mw' in col or 'capacity' in col]
                        capacity = None
                        
                        for col in capacity_cols:
                            if pd.notna(row.get(col)):
                                try:
                                    capacity = float(str(row[col]).replace(',', ''))
                                    if capacity >= self.min_capacity_mw:
                                        break
                                except (ValueError, TypeError):
                                    continue
                        
                        if not capacity or capacity < self.min_capacity_mw:
                            continue
                        
                        request_data = {
                            'request_id': f"CAISO_{row.get('queue_position', 'UNK')}",
                            'project_name': str(row.get('project_name', row.get('generator_name', ''))),
                            'capacity_mw': capacity,
                            'location': str(row.get('county', '')),
                            'state': 'CA',
                            'customer': str(row.get('customer', '')),
                            'utility': 'CAISO',
                            'status': row.get('status', 'Active'),
                            'source': 'CAISO',
                            'source_url': 'http://www.caiso.com/Documents/AllGeneratorInterconnectionQueue.xls'
                        }
                        
                        requests_found.append(request_data)
                        
                    except Exception as e:
                        logger.warning(f"Error processing CAISO row: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error monitoring CAISO: {e}")
        
        return requests_found

    def monitor_ferc(self) -> List[Dict]:
        """Monitor FERC eLibrary"""
        logger.info("Monitoring FERC...")
        requests_found = []
        
        try:
            # Search for recent interconnection filings
            search_terms = ['interconnection "100 MW"', 'interconnection "200 MW"']
            
            for search_term in search_terms:
                # Simulate FERC search - in real implementation, this would parse FERC's search results
                sample_filings = [
                    {
                        'accession_number': f'20241201-{hash(search_term) % 9999:04d}',
                        'title': f'Interconnection Agreement - {search_term}',
                        'filing_date': datetime.now().strftime('%Y-%m-%d'),
                        'company': 'Regional Utility Company'
                    }
                ]
                
                for filing in sample_filings:
                    capacity = self.extract_capacity(filing['title'])
                    
                    if capacity and capacity >= self.min_capacity_mw:
                        request_data = {
                            'request_id': f"FERC_{filing['accession_number']}",
                            'project_name': filing['title'],
                            'capacity_mw': capacity,
                            'utility': filing.get('company', ''),
                            'request_date': filing['filing_date'],
                            'status': 'Filed',
                            'source': 'FERC',
                            'source_url': f"https://elibrary.ferc.gov/eLibrary/filelist?accession_number={filing['accession_number']}"
                        }
                        
                        requests_found.append(request_data)
                
                time.sleep(2)  # Be respectful
                
        except Exception as e:
            logger.error(f"Error monitoring FERC: {e}")
        
        return requests_found

    def monitor_pjm(self) -> List[Dict]:
        """Monitor PJM queue"""
        logger.info("Monitoring PJM...")
        requests_found = []
        
        try:
            # Simulate PJM data - in real implementation, this would download and parse PJM queue files
            sample_requests = [
                {
                    'queue_id': 'AB1-234',
                    'project_name': 'Northern Virginia Data Center Load',
                    'capacity': 250.0,
                    'state': 'VA',
                    'location': 'Loudoun County',
                    'customer': 'Tech Company LLC',
                    'status': 'Under Study'
                }
            ]
            
            for req in sample_requests:
                if req['capacity'] >= self.min_capacity_mw:
                    request_data = {
                        'request_id': f"PJM_{req['queue_id']}",
                        'project_name': req['project_name'],
                        'capacity_mw': req['capacity'],
                        'state': req['state'],
                        'location': req['location'],
                        'customer': req['customer'],
                        'status': req['status'],
                        'source': 'PJM',
                        'source_url': 'https://www.pjm.com/markets-and-operations/interconnections'
                    }
                    
                    requests_found.append(request_data)
                    
        except Exception as e:
            logger.error(f"Error monitoring PJM: {e}")
        
        return requests_found

    def store_request(self, request_data: Dict) -> bool:
        """Store power request with duplicate detection"""
        try:
            # Generate hash for duplicate detection
            data_hash = self.generate_data_hash(request_data)
            
            # Check for existing request
            existing = PowerRequest.query.filter_by(data_hash=data_hash).first()
            if existing:
                return False  # Duplicate
            
            # Classify project type
            project_type = self.classify_project_type(
                request_data.get('project_name', ''),
                request_data.get('customer', '')
            )
            
            # Geocode location if provided
            latitude, longitude = None, None
            if request_data.get('location') and request_data.get('state'):
                latitude, longitude = self.geocode_location(
                    request_data['location'], 
                    request_data['state']
                )
            
            # Create new request
            power_request = PowerRequest(
                request_id=request_data.get('request_id'),
                project_name=request_data.get('project_name'),
                capacity_mw=request_data.get('capacity_mw'),
                location=request_data.get('location'),
                state=request_data.get('state'),
                utility=request_data.get('utility'),
                customer=request_data.get('customer'),
                request_date=datetime.fromisoformat(request_data['request_date']) if request_data.get('request_date') else None,
                status=request_data.get('status'),
                project_type=project_type,
                source=request_data.get('source'),
                source_url=request_data.get('source_url'),
                data_hash=data_hash,
                latitude=latitude,
                longitude=longitude
            )
            
            db.session.add(power_request)
            db.session.commit()
            
            logger.info(f"Stored new request: {request_data.get('request_id')}")
            return True
            
        except Exception as e:
            logger.error(f"Error storing request: {e}")
            db.session.rollback()
            return False

    def run_monitoring_cycle(self) -> Dict:
        """Run complete monitoring cycle"""
        start_time = datetime.utcnow()
        logger.info("Starting monitoring cycle...")
        
        all_requests = []
        
        # Monitor each source
        monitors = [
            ('CAISO', self.monitor_caiso),
            ('FERC', self.monitor_ferc),
            ('PJM', self.monitor_pjm)
        ]
        
        for source_name, monitor_func in monitors:
            try:
                source_requests = monitor_func()
                all_requests.extend(source_requests)
                logger.info(f"Found {len(source_requests)} requests from {source_name}")
            except Exception as e:
                logger.error(f"Error monitoring {source_name}: {e}")
        
        # Store new requests
        stored_count = 0
        new_requests = []
        
        for request in all_requests:
            if self.store_request(request):
                stored_count += 1
                new_requests.append(request)
        
        # Send email alerts if new high-value requests found
        if new_requests:
            try:
                send_email_alerts(new_requests)
            except Exception as e:
                logger.error(f"Error sending alerts: {e}")
        
        duration = (datetime.utcnow() - start_time).total_seconds()
        
        # Record monitoring run
        monitoring_run = MonitoringRun(
            sources_checked=len(monitors),
            requests_found=len(all_requests),
            requests_stored=stored_count,
            duration_seconds=duration,
            status='completed'
        )
        
        db.session.add(monitoring_run)
        db.session.commit()
        
        results = {
            'total_requests_found': len(all_requests),
            'requests_stored': stored_count,
            'duration_seconds': duration,
            'sources_checked': len(monitors),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        logger.info(f"Monitoring cycle complete: {stored_count} new requests stored")
        return results

# ============================================================================
# EMAIL ALERTS
# ============================================================================

def send_email_alerts(new_requests: List[Dict]):
    """Send email alerts using Resend"""
    resend_api_key = os.environ.get('RESEND_API_KEY')
    from_email = os.environ.get('FROM_EMAIL', 'alerts@powermonitor.com')
    
    if not resend_api_key:
        logger.warning("No Resend API key - skipping email alerts")
        return
    
    # Get active subscriptions
    subscriptions = AlertSubscription.query.filter_by(active=True).all()
    
    for subscription in subscriptions:
        try:
            # Filter requests for this subscription
            filtered_requests = []
            
            for req in new_requests:
                # Check capacity threshold
                if req.get('capacity_mw', 0) < subscription.min_capacity:
                    continue
                
                # Check state filter
                if subscription.states:
                    allowed_states = json.loads(subscription.states)
                    if req.get('state') and req.get('state') not in allowed_states:
                        continue
                
                filtered_requests.append(req)
            
            if not filtered_requests:
                continue
            
            # Send email
            subject = f"⚡ {len(filtered_requests)} New Power Project(s) - {datetime.now().strftime('%Y-%m-%d')}"
            
            html_content = f"""
            <h2>New Power Projects Alert</h2>
            <p>{len(filtered_requests)} new project(s) found matching your criteria:</p>
            <ul>
            """
            
            for req in filtered_requests:
                html_content += f"""
                <li><strong>{req.get('project_name', 'Unnamed Project')}</strong><br>
                    Capacity: {req.get('capacity_mw')} MW<br>
                    Location: {req.get('location', '')}, {req.get('state', '')}<br>
                    Customer: {req.get('customer', 'Unknown')}<br>
                    Source: {req.get('source')}<br><br>
                </li>
                """
            
            html_content += "</ul><p>Visit your dashboard for more details.</p>"
            
            # Send via Resend
            headers = {
                'Authorization': f'Bearer {resend_api_key}',
                'Content-Type': 'application/json'
            }
            
            data = {
                'from': from_email,
                'to': [subscription.email],
                'subject': subject,
                'html': html_content
            }
            
            response = requests.post(
                'https://api.resend.com/emails',
                headers=headers,
                json=data,
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info(f"Email alert sent to {subscription.email}")
            else:
                logger.error(f"Failed to send email to {subscription.email}: {response.text}")
                
        except Exception as e:
            logger.error(f"Error sending alert to {subscription.email}: {e}")

# ============================================================================
# FLASK ROUTES
# ============================================================================

@app.route('/')
def index():
    """Main dashboard"""
    total_requests = PowerRequest.query.count()
    recent_requests = PowerRequest.query.filter(
        PowerRequest.created_at >= datetime.utcnow() - timedelta(days=30)
    ).count()
    
    high_capacity = PowerRequest.query.filter(PowerRequest.capacity_mw >= 200).count()
    datacenter_requests = PowerRequest.query.filter(PowerRequest.project_type == 'datacenter').count()
    
    # Get recent monitoring runs
    recent_runs = MonitoringRun.query.order_by(MonitoringRun.run_date.desc()).limit(5).all()
    
    # Get top states by capacity
    from sqlalchemy import func
    top_states = db.session.query(
        PowerRequest.state, 
        func.count(PowerRequest.id).label('count'),
        func.sum(PowerRequest.capacity_mw).label('total_capacity')
    ).filter(
        PowerRequest.state.isnot(None)
    ).group_by(
        PowerRequest.state
    ).order_by(
        func.sum(PowerRequest.capacity_mw).desc()
    ).limit(10).all()
    
    return render_template('index.html',
                         total_requests=total_requests,
                         recent_requests=recent_requests,
                         high_capacity=high_capacity,
                         datacenter_requests=datacenter_requests,
                         recent_runs=recent_runs,
                         top_states=top_states)

@app.route('/projects')
def projects():
    """Projects listing with filtering"""
    page = request.args.get('page', 1, type=int)
    per_page = 50
    
    # Build query with filters
    query = PowerRequest.query
    
    min_capacity = request.args.get('min_capacity', type=float)
    state = request.args.get('state')
    project_type = request.args.get('project_type')
    search = request.args.get('search')
    
    if min_capacity:
        query = query.filter(PowerRequest.capacity_mw >= min_capacity)
    if state:
        query = query.filter(PowerRequest.state == state)
    if project_type:
        query = query.filter(PowerRequest.project_type == project_type)
    if search:
        search_term = f"%{search}%"
        query = query.filter(
            db.or_(
                PowerRequest.project_name.ilike(search_term),
                PowerRequest.customer.ilike(search_term),
                PowerRequest.location.ilike(search_term)
            )
        )
    
    # Paginate
    projects_page = query.order_by(PowerRequest.created_at.desc()).paginate(
        page=page, per_page=per_page, error_out=False
    )
    
    # Get filter options
    states = db.session.query(PowerRequest.state).distinct().filter(
        PowerRequest.state.isnot(None)
    ).all()
    
    project_types = db.session.query(PowerRequest.project_type).distinct().filter(
        PowerRequest.project_type.isnot(None)
    ).all()
    
    return render_template('projects.html',
                         projects=projects_page,
                         states=[s[0] for s in states],
                         project_types=[pt[0] for pt in project_types],
                         current_filters=request.args)

@app.route('/alerts')
def alerts():
    """Alert management"""
    subscriptions = AlertSubscription.query.filter_by(active=True).all()
    return render_template('alerts.html', subscriptions=subscriptions)

@app.route('/alerts/subscribe', methods=['POST'])
def subscribe_alerts():
    """Subscribe to email alerts"""
    email = request.form.get('email')
    min_capacity = request.form.get('min_capacity', 200, type=float)
    states = request.form.getlist('states')
    
    if not email:
        flash('Email address is required', 'error')
        return redirect(url_for('alerts'))
    
    # Check if subscription exists
    existing = AlertSubscription.query.filter_by(email=email, active=True).first()
    if existing:
        flash('Email address already subscribed', 'warning')
        return redirect(url_for('alerts'))
    
    subscription = AlertSubscription(
        email=email,
        min_capacity=min_capacity,
        states=json.dumps(states) if states else None
    )
    
    db.session.add(subscription)
    db.session.commit()
    
    flash('Successfully subscribed to alerts!', 'success')
    return redirect(url_for('alerts'))

@app.route('/monitoring')
def monitoring():
    """Monitoring status"""
    runs = MonitoringRun.query.order_by(MonitoringRun.run_date.desc()).limit(20).all()
    
    # Get source statistics
    from sqlalchemy import func
    source_stats = db.session.query(
        PowerRequest.source,
        func.count(PowerRequest.id).label('count'),
        func.max(PowerRequest.created_at).label('last_update')
    ).group_by(PowerRequest.source).all()
    
    return render_template('monitoring.html', runs=runs, source_stats=source_stats)

@app.route('/run-monitor', methods=['POST'])
def run_monitor():
    """Manual monitoring trigger"""
    try:
        monitor = PowerUtilityMonitor()
        result = monitor.run_monitoring_cycle()
        flash(f'Monitoring completed: {result["requests_stored"]} new requests found', 'success')
    except Exception as e:
        flash(f'Monitoring failed: {str(e)}', 'error')
    
    return redirect(url_for('monitoring'))

@app.route('/api/projects')
def api_projects():
    """API endpoint for projects"""
    projects = PowerRequest.query.order_by(PowerRequest.created_at.desc()).limit(1000).all()
    return jsonify([p.to_dict() for p in projects])

@app.route('/export/<format>')
def export_data(format):
    """Export data"""
    if format not in ['csv', 'json']:
        return jsonify({'error': 'Invalid format'}), 400
    
    projects = PowerRequest.query.order_by(PowerRequest.created_at.desc()).all()
    
    if format == 'json':
        return jsonify([p.to_dict() for p in projects])
    
    elif format == 'csv':
        output = io.StringIO()
        fieldnames = ['request_id', 'project_name', 'capacity_mw', 'location', 'state', 
                     'utility', 'customer', 'status', 'project_type', 'source', 'created_at']
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for project in projects:
            writer.writerow({
                'request_id': project.request_id,
                'project_name': project.project_name,
                'capacity_mw': project.capacity_mw,
                'location': project.location,
                'state': project.state,
                'utility': project.utility,
                'customer': project.customer,
                'status': project.status,
                'project_type': project.project_type,
                'source': project.source,
                'created_at': project.created_at.isoformat() if project.created_at else ''
            })
        
        response = make_response(output.getvalue())
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename=power_projects_{datetime.now().strftime("%Y%m%d")}.csv'
        return response

# Special endpoint for Railway cron job
@app.route('/cron/daily-monitor')
def cron_daily_monitor():
    """Endpoint for Railway cron job to trigger daily monitoring"""
    try:
        monitor = PowerUtilityMonitor()
        result = monitor.run_monitoring_cycle()
        return jsonify({
            'status': 'success',
            'message': f'Monitoring completed: {result["requests_stored"]} new requests stored',
            'result': result
        })
    except Exception as e:
        logger.error(f"Cron monitoring failed: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

# Initialize database
@app.route('/init')
def init_database():
    """Initialize database - call once after deployment"""
    db.create_all()
    return "Database initialized successfully!"

# Create tables when app starts
with app.app_context():
    db.create_all()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))