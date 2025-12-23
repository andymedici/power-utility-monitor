# app.py - Complete Power Utility Monitor with Ultra Features
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

# Fix Railway PostgreSQL URL
if 'DATABASE_URL' in os.environ:
    if os.environ['DATABASE_URL'].startswith('postgres://'):
        os.environ['DATABASE_URL'] = os.environ['DATABASE_URL'].replace('postgres://', 'postgresql://', 1)

from flask import Flask, jsonify, request, render_template, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_, and_

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder='templates')

# Add min/max to Jinja2 for templates
app.jinja_env.globals.update(min=min, max=max)

app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key-change-in-production')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# ==================== DATABASE MODELS ====================

class PowerProject(db.Model):
    __tablename__ = 'power_projects'
    
    id = db.Column(db.Integer, primary_key=True)
    request_id = db.Column(db.String(255), unique=True, index=True)
    queue_position = db.Column(db.String(100))
    project_name = db.Column(db.String(500))
    capacity_mw = db.Column(db.Float, index=True)
    location = db.Column(db.String(500))
    county = db.Column(db.String(200), index=True)
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
    
    # Hunter scoring fields
    hunter_score = db.Column(db.Integer, default=0, index=True)
    hunter_notes = db.Column(db.Text)
    
    source = db.Column(db.String(100), index=True)
    source_url = db.Column(db.Text)
    data_hash = db.Column(db.String(32), unique=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    @classmethod
    def active(cls):
        """Return only projects from last 90 days"""
        cutoff = datetime.utcnow() - timedelta(days=90)
        return cls.query.filter(cls.created_at >= cutoff)


class MonitoringRun(db.Model):
    __tablename__ = 'monitoring_runs'
    
    id = db.Column(db.Integer, primary_key=True)
    run_date = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    sources_checked = db.Column(db.Integer, default=0)
    projects_found = db.Column(db.Integer, default=0)
    projects_stored = db.Column(db.Integer, default=0)
    duration_seconds = db.Column(db.Float)
    status = db.Column(db.String(50))
    details = db.Column(db.Text)


class EmailAlert(db.Model):
    __tablename__ = 'email_alerts'
    
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, index=True)
    min_capacity = db.Column(db.Integer, default=200)
    states = db.Column(db.String(500))  # Comma-separated
    active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_alert = db.Column(db.DateTime)


# ==================== MAIN MONITORING FUNCTION ====================

def run_full_scan():
    """Run full scan with ultra monitor"""
    from ultra_monitor import UltraPowerMonitor
    
    start_time = time.time()
    monitor = UltraPowerMonitor()
    
    try:
        result = monitor.run_ultra_monitoring(max_workers=4)
        
        # Store projects in database
        stored = 0
        updated = 0
        
        for project_data in result['all_projects']:
            try:
                existing = PowerProject.query.filter_by(
                    data_hash=project_data['data_hash']
                ).first()
                
                if existing:
                    # Update if score changed or status changed
                    if (existing.hunter_score != project_data.get('hunter_score') or 
                        existing.status != project_data.get('status')):
                        existing.hunter_score = project_data.get('hunter_score', 0)
                        existing.hunter_notes = project_data.get('hunter_notes', '')
                        existing.status = project_data.get('status', '')
                        existing.last_updated = datetime.utcnow()
                        updated += 1
                else:
                    # Fill in missing fields
                    for field in ['queue_position', 'location', 'county', 'customer', 
                                 'developer', 'utility', 'status', 'fuel_type', 
                                 'interconnection_point', 'hunter_score', 'hunter_notes']:
                        if field not in project_data:
                            project_data[field] = '' if field not in ['hunter_score'] else 0
                    
                    project = PowerProject(**project_data)
                    db.session.add(project)
                    stored += 1
                    
                    # Check if this should trigger alerts
                    if project_data.get('hunter_score', 0) >= 70:
                        send_alerts_for_project(project_data)
            
            except Exception as e:
                logger.error(f"Error storing project: {e}")
        
        db.session.commit()
        
        # Clean up old projects (>90 days)
        cutoff = datetime.utcnow() - timedelta(days=90)
        old_count = PowerProject.query.filter(PowerProject.created_at < cutoff).delete()
        db.session.commit()
        
        duration = time.time() - start_time
        
        # Record monitoring run
        run = MonitoringRun(
            sources_checked=result['sources_checked'],
            projects_found=result['projects_found'],
            projects_stored=stored,
            duration_seconds=duration,
            status='completed',
            details=json.dumps(result['by_source'])
        )
        db.session.add(run)
        db.session.commit()
        
        return {
            **result,
            'projects_stored': stored,
            'projects_updated': updated,
            'old_projects_removed': old_count
        }
    
    except Exception as e:
        logger.error(f"Monitoring error: {e}", exc_info=True)
        
        run = MonitoringRun(
            sources_checked=0,
            projects_found=0,
            projects_stored=0,
            duration_seconds=time.time() - start_time,
            status='failed',
            details=str(e)
        )
        db.session.add(run)
        db.session.commit()
        
        raise


def send_alerts_for_project(project_data):
    """Send email alerts for high-confidence projects"""
    try:
        alerts = EmailAlert.query.filter_by(active=True).all()
        
        for alert in alerts:
            # Check capacity threshold
            if project_data.get('capacity_mw', 0) < alert.min_capacity:
                continue
            
            # Check state filter
            if alert.states:
                allowed_states = alert.states.split(',')
                if project_data.get('state') not in allowed_states:
                    continue
            
            # Send email (implement with SendGrid, AWS SES, or similar)
            # For now, just log
            logger.info(f"Would send alert to {alert.email} for project: {project_data.get('project_name')}")
            
            alert.last_alert = datetime.utcnow()
        
        db.session.commit()
    
    except Exception as e:
        logger.error(f"Alert error: {e}")


# ==================== FLASK ROUTES ====================

@app.route('/')
def index():
    """Dashboard homepage"""
    try:
        total = PowerProject.active().count()
        
        # High confidence data centers
        suspected_dc = PowerProject.active().filter(
            PowerProject.hunter_score >= 70
        ).count()
        
        # Top hunters
        top_hunters = PowerProject.active().filter(
            PowerProject.hunter_score >= 60
        ).order_by(PowerProject.hunter_score.desc()).limit(10).all()
        
        # Recent monitoring runs
        logs = MonitoringRun.query.order_by(
            MonitoringRun.run_date.desc()
        ).limit(5).all()
        
        # Source statistics
        last_run = MonitoringRun.query.order_by(
            MonitoringRun.run_date.desc()
        ).first()
        
        source_stats = {}
        if last_run and last_run.details:
            try:
                source_stats = json.loads(last_run.details)
            except:
                pass
        
        return render_template('index.html',
            total=total,
            suspected_dc=suspected_dc,
            top_hunters=top_hunters,
            logs=logs,
            source_stats=source_stats
        )
    
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        return f"Error loading dashboard: {str(e)}", 500


@app.route('/projects')
def projects():
    """Projects listing with filters"""
    try:
        page = request.args.get('page', 1, type=int)
        filter_type = request.args.get('filter', 'all')
        state_filter = request.args.get('state', '')
        min_capacity = request.args.get('min_capacity', type=int)
        search = request.args.get('search', '')
        
        query = PowerProject.active()
        
        # Apply filters
        if filter_type == 'hunter':
            query = query.filter(PowerProject.hunter_score >= 60)
        elif filter_type == 'load':
            query = query.filter(PowerProject.capacity_mw >= 100)
        elif filter_type == 'datacenter':
            query = query.filter(PowerProject.project_type == 'datacenter')
        
        if state_filter:
            query = query.filter(PowerProject.state == state_filter.upper())
        
        if min_capacity:
            query = query.filter(PowerProject.capacity_mw >= min_capacity)
        
        if search:
            search_term = f"%{search}%"
            query = query.filter(
                or_(
                    PowerProject.project_name.ilike(search_term),
                    PowerProject.customer.ilike(search_term),
                    PowerProject.county.ilike(search_term)
                )
            )
        
        # Order by hunter score, then capacity
        query = query.order_by(
            PowerProject.hunter_score.desc(),
            PowerProject.capacity_mw.desc()
        )
        
        pagination = query.paginate(page=page, per_page=50, error_out=False)
        
        # Get available states for filter
        states = db.session.query(PowerProject.state).distinct().all()
        states = [s[0] for s in states if s[0]]
        
        return render_template('projects.html',
            pagination=pagination,
            filter_type=filter_type,
            state_filter=state_filter,
            min_capacity=min_capacity,
            search=search,
            states=sorted(states)
        )
    
    except Exception as e:
        logger.error(f"Projects error: {e}")
        return f"Error loading projects: {str(e)}", 500


@app.route('/project/<int:id>')
def project_detail(id):
    """Individual project details"""
    try:
        project = PowerProject.query.get_or_404(id)
        
        # Find similar projects (same state, similar capacity, high hunter score)
        similar = PowerProject.active().filter(
            and_(
                PowerProject.state == project.state,
                PowerProject.capacity_mw >= project.capacity_mw * 0.7,
                PowerProject.capacity_mw <= project.capacity_mw * 1.3,
                PowerProject.hunter_score >= 40,
                PowerProject.id != project.id
            )
        ).order_by(PowerProject.hunter_score.desc()).limit(5).all()
        
        return render_template('project_detail.html',
            project=project,
            similar=similar
        )
    
    except Exception as e:
        logger.error(f"Project detail error: {e}")
        return f"Error loading project: {str(e)}", 500


@app.route('/monitoring')
def monitoring():
    """Monitoring dashboard"""
    try:
        runs = MonitoringRun.query.order_by(
            MonitoringRun.run_date.desc()
        ).limit(50).all()
        
        # Calculate source health
        last_run = runs[0] if runs else None
        source_stats = {}
        
        if last_run and last_run.details:
            try:
                source_stats = json.loads(last_run.details)
            except:
                pass
        
        return render_template('monitoring.html',
            runs=runs,
            source_stats=source_stats
        )
    
    except Exception as e:
        logger.error(f"Monitoring page error: {e}")
        return f"Error: {str(e)}", 500


@app.route('/alerts')
def alerts():
    """Email alerts page"""
    try:
        subscriptions = EmailAlert.query.filter_by(active=True).all()
        
        return render_template('alerts.html',
            subscriptions=subscriptions
        )
    
    except Exception as e:
        logger.error(f"Alerts page error: {e}")
        return f"Error: {str(e)}", 500


@app.route('/alerts/subscribe', methods=['POST'])
def subscribe_alerts():
    """Subscribe to email alerts"""
    try:
        email = request.form.get('email', '').strip()
        min_capacity = request.form.get('min_capacity', 200, type=int)
        states = ','.join(request.form.getlist('states'))
        
        if not email:
            flash('Email address is required', 'error')
            return redirect(url_for('alerts'))
        
        # Check if already subscribed
        existing = EmailAlert.query.filter_by(email=email).first()
        
        if existing:
            existing.min_capacity = min_capacity
            existing.states = states
            existing.active = True
            flash('Subscription updated!', 'success')
        else:
            alert = EmailAlert(
                email=email,
                min_capacity=min_capacity,
                states=states
            )
            db.session.add(alert)
            flash('Successfully subscribed to alerts!', 'success')
        
        db.session.commit()
        return redirect(url_for('alerts'))
    
    except Exception as e:
        logger.error(f"Subscribe error: {e}")
        flash(f'Error: {str(e)}', 'error')
        return redirect(url_for('alerts'))


@app.route('/run-monitor', methods=['GET', 'POST'])
def trigger_monitor():
    """Manually trigger monitoring scan"""
    try:
        logger.info("Manual scan triggered")
        result = run_full_scan()
        
        return render_template('scan_results.html', result=result)
    
    except Exception as e:
        logger.error(f"Scan error: {e}")
        return render_template('scan_results.html', 
            result={'error': str(e)}
        )


@app.route('/analytics')
def analytics():
    """Analytics dashboard"""
    try:
        # Top states by project count
        state_stats = db.session.query(
            PowerProject.state,
            db.func.count(PowerProject.id).label('count'),
            db.func.sum(PowerProject.capacity_mw).label('total_mw')
        ).filter(
            PowerProject.created_at >= datetime.utcnow() - timedelta(days=90)
        ).group_by(
            PowerProject.state
        ).order_by(
            db.desc('count')
        ).limit(15).all()
        
        # Top counties (data center hotspots)
        hotspot_stats = db.session.query(
            PowerProject.county,
            PowerProject.state,
            db.func.count(PowerProject.id).label('count'),
            db.func.avg(PowerProject.hunter_score).label('avg_score')
        ).filter(
            and_(
                PowerProject.created_at >= datetime.utcnow() - timedelta(days=90),
                PowerProject.hunter_score >= 40
            )
        ).group_by(
            PowerProject.county,
            PowerProject.state
        ).order_by(
            db.desc('count')
        ).limit(15).all()
        
        # Timeline of discoveries
        timeline = db.session.query(
            db.func.date(PowerProject.created_at).label('date'),
            db.func.count(PowerProject.id).label('count')
        ).filter(
            PowerProject.created_at >= datetime.utcnow() - timedelta(days=30)
        ).group_by(
            db.func.date(PowerProject.created_at)
        ).order_by('date').all()
        
        # Hunter score distribution
        score_distribution = {
            'high': PowerProject.active().filter(PowerProject.hunter_score >= 70).count(),
            'medium': PowerProject.active().filter(
                and_(PowerProject.hunter_score >= 40, PowerProject.hunter_score < 70)
            ).count(),
            'low': PowerProject.active().filter(PowerProject.hunter_score < 40).count()
        }
        
        return render_template('analytics.html',
            state_stats=state_stats,
            hotspot_stats=hotspot_stats,
            timeline=timeline,
            score_distribution=score_distribution
        )
    
    except Exception as e:
        logger.error(f"Analytics error: {e}")
        return f"Error: {str(e)}", 500


# ==================== API ENDPOINTS ====================

@app.route('/api/projects')
def api_projects():
    """API endpoint for projects"""
    try:
        min_score = request.args.get('min_score', 0, type=int)
        state = request.args.get('state', '')
        limit = request.args.get('limit', 100, type=int)
        
        query = PowerProject.active().filter(PowerProject.hunter_score >= min_score)
        
        if state:
            query = query.filter(PowerProject.state == state.upper())
        
        query = query.order_by(PowerProject.hunter_score.desc()).limit(limit)
        
        projects = query.all()
        
        return jsonify([{
            'id': p.id,
            'request_id': p.request_id,
            'name': p.project_name,
            'customer': p.customer,
            'capacity_mw': p.capacity_mw,
            'county': p.county,
            'state': p.state,
            'hunter_score': p.hunter_score,
            'hunter_notes': p.hunter_notes,
            'source': p.source,
            'status': p.status,
            'created_at': p.created_at.isoformat() if p.created_at else None
        } for p in projects])
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stats')
def api_stats():
    """API endpoint for statistics"""
    try:
        stats = {
            'total_projects': PowerProject.active().count(),
            'high_confidence_dc': PowerProject.active().filter(
                PowerProject.hunter_score >= 70
            ).count(),
            'medium_confidence_dc': PowerProject.active().filter(
                and_(PowerProject.hunter_score >= 40, PowerProject.hunter_score < 70)
            ).count(),
            'total_capacity_mw': db.session.query(
                db.func.sum(PowerProject.capacity_mw)
            ).filter(
                PowerProject.created_at >= datetime.utcnow() - timedelta(days=90)
            ).scalar() or 0,
            'last_scan': None
        }
        
        last_run = MonitoringRun.query.order_by(
            MonitoringRun.run_date.desc()
        ).first()
        
        if last_run:
            stats['last_scan'] = last_run.run_date.isoformat()
        
        return jsonify(stats)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/export/csv')
def export_csv():
    """Export projects to CSV"""
    try:
        min_score = request.args.get('min_score', 0, type=int)
        
        projects = PowerProject.active().filter(
            PowerProject.hunter_score >= min_score
        ).order_by(PowerProject.hunter_score.desc()).all()
        
        csv_data = "Request ID,Project Name,Customer,Capacity (MW),County,State,Hunter Score,Signals,Status,Source,Date Added\n"
        
        for p in projects:
            csv_data += f'"{p.request_id}","{p.project_name or ""}","{p.customer or ""}",{p.capacity_mw},"{p.county or ""}","{p.state or ""}",{p.hunter_score},"{p.hunter_notes or ""}","{p.status or ""}","{p.source}",{p.created_at.strftime("%Y-%m-%d")}\n'
        
        response = app.response_class(
            csv_data,
            mimetype='text/csv',
            headers={
                "Content-Disposition": f"attachment;filename=power_projects_{datetime.now().strftime('%Y%m%d')}.csv"
            }
        )
        return response
    
    except Exception as e:
        logger.error(f"Export error: {e}")
        return f"Error: {str(e)}", 500


@app.route('/init-db')
def init_db():
    """Initialize database"""
    try:
        db.create_all()
        return "✅ Database initialized! <a href='/'>Go to Dashboard</a>"
    except Exception as e:
        return f"❌ Error: {str(e)}", 500


@app.route('/reset-db')
def reset_database():
    """Reset database (USE WITH CAUTION)"""
    try:
        db.drop_all()
        db.create_all()
        return "✅ Database reset complete! <a href='/'>Dashboard</a> | <a href='/run-monitor'>Run Scan</a>"
    except Exception as e:
        return f"❌ Error: {str(e)}", 500


# ==================== ERROR HANDLERS ====================

@app.errorhandler(404)
def not_found(e):
    return render_template('404.html'), 404


@app.errorhandler(500)
def server_error(e):
    logger.error(f"Server error: {e}")
    return render_template('500.html'), 500


# ==================== STARTUP ====================

with app.app_context():
    try:
        db.create_all()
        logger.info("✅ Database ready")
        
        # Check if we have data
        count = PowerProject.query.count()
        if count == 0:
            logger.info("📊 No projects in database. Run a scan to populate.")
        else:
            logger.info(f"📊 Database has {count} projects")
    
    except Exception as e:
        logger.error(f"❌ Database initialization error: {e}")


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)
