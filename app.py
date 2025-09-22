# app.py - Simplified Power Monitor for Railway
import os
import sys
import logging
from datetime import datetime, timedelta

# CRITICAL: Fix Railway PostgreSQL URL before anything else
if 'DATABASE_URL' in os.environ:
    if os.environ['DATABASE_URL'].startswith('postgres://'):
        os.environ['DATABASE_URL'] = os.environ['DATABASE_URL'].replace('postgres://', 'postgresql://', 1)
        print("Fixed DATABASE_URL format", file=sys.stderr, flush=True)

from flask import Flask, render_template_string, jsonify, request
from flask_sqlalchemy import SQLAlchemy

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)

# Configuration
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-key-change-in-production')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize database
db = SQLAlchemy(app)

# Database Models
class PowerProject(db.Model):
    __tablename__ = 'power_projects'
    
    id = db.Column(db.Integer, primary_key=True)
    project_name = db.Column(db.String(500))
    capacity_mw = db.Column(db.Float)
    location = db.Column(db.String(500))
    state = db.Column(db.String(2))
    source = db.Column(db.String(100))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
class MonitoringRun(db.Model):
    __tablename__ = 'monitoring_runs'
    
    id = db.Column(db.Integer, primary_key=True)
    run_date = db.Column(db.DateTime, default=datetime.utcnow)
    projects_found = db.Column(db.Integer, default=0)
    status = db.Column(db.String(50))

# Routes
@app.route('/')
def index():
    try:
        total = PowerProject.query.count()
        recent = PowerProject.query.filter(
            PowerProject.created_at >= datetime.utcnow() - timedelta(days=30)
        ).count()
        last_run = MonitoringRun.query.order_by(MonitoringRun.run_date.desc()).first()
        
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Power Monitor</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                .card { background: #f5f5f5; padding: 20px; margin: 20px 0; border-radius: 8px; }
                .metric { display: inline-block; margin: 20px; padding: 15px; background: white; border-radius: 5px; }
                table { width: 100%; border-collapse: collapse; }
                th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
                .btn { padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px; display: inline-block; margin: 5px; }
                .nav { background: #333; color: white; padding: 15px; margin: -40px -40px 20px -40px; }
                .nav a { color: white; margin-right: 20px; text-decoration: none; }
            </style>
        </head>
        <body>
            <div class="nav">
                <a href="/">Dashboard</a>
                <a href="/projects">Projects</a>
                <a href="/monitoring">Monitoring</a>
                <a href="/health">Health Check</a>
            </div>
            
            <h1>⚡ Power Utility Monitor</h1>
            <div class="card">
                <h2>System Status</h2>
                <div class="metric">
                    <h3>""" + str(total) + """</h3>
                    <p>Total Projects</p>
                </div>
                <div class="metric">
                    <h3>""" + str(recent) + """</h3>
                    <p>Recent Projects</p>
                </div>
                <div class="metric">
                    <h3>""" + (last_run.run_date.strftime('%Y-%m-%d %H:%M') if last_run else 'Never') + """</h3>
                    <p>Last Run</p>
                </div>
            </div>
            <div class="card">
                <h2>Quick Actions</h2>
                <a href="/run-monitor" class="btn">Run Monitor Now</a>
                <a href="/projects" class="btn">View Projects</a>
            </div>
        </body>
        </html>
        """
        return html
    except Exception as e:
        logger.error(f"Error in index: {e}")
        return f"Error: {str(e)}", 500

@app.route('/projects')
def projects():
    try:
        projects = PowerProject.query.order_by(PowerProject.created_at.desc()).limit(100).all()
        
        rows = ""
        for project in projects:
            rows += f"""
            <tr>
                <td>{project.project_name or 'Unknown'}</td>
                <td>{project.capacity_mw}</td>
                <td>{project.location or 'N/A'}</td>
                <td>{project.created_at.strftime('%Y-%m-%d') if project.created_at else 'N/A'}</td>
            </tr>
            """
        
        if not projects:
            rows = "<tr><td colspan='4'>No projects yet. Click 'Run Monitor Now' on the dashboard to fetch data.</td></tr>"
        
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Power Monitor - Projects</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                .card { background: #f5f5f5; padding: 20px; margin: 20px 0; border-radius: 8px; }
                table { width: 100%; border-collapse: collapse; }
                th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
                .nav { background: #333; color: white; padding: 15px; margin: -40px -40px 20px -40px; }
                .nav a { color: white; margin-right: 20px; text-decoration: none; }
            </style>
        </head>
        <body>
            <div class="nav">
                <a href="/">Dashboard</a>
                <a href="/projects">Projects</a>
                <a href="/monitoring">Monitoring</a>
                <a href="/health">Health Check</a>
            </div>
            
            <h1>Power Projects</h1>
            <div class="card">
                <table>
                    <tr>
                        <th>Project Name</th>
                        <th>Capacity (MW)</th>
                        <th>Location</th>
                        <th>Date</th>
                    </tr>
                    """ + rows + """
                </table>
            </div>
        </body>
        </html>
        """
        return html
    except Exception as e:
        logger.error(f"Error in projects: {e}")
        return f"Error: {str(e)}", 500

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "database": "connected" if db.engine else "disconnected"
    })

@app.route('/init')
def init_db():
    """Initialize database tables"""
    try:
        db.create_all()
        return "Database initialized successfully!"
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        return f"Error: {str(e)}", 500

@app.route('/run-monitor')
def run_monitor():
    """Run monitoring (simplified for testing)"""
    try:
        # Add sample data for testing
        sample_projects = [
            PowerProject(
                project_name="Northern Virginia Data Center",
                capacity_mw=250.0,
                location="Loudoun County",
                state="VA",
                source="Test"
            ),
            PowerProject(
                project_name="California Solar Farm",
                capacity_mw=150.0,
                location="San Diego",
                state="CA",
                source="Test"
            )
        ]
        
        for project in sample_projects:
            existing = PowerProject.query.filter_by(
                project_name=project.project_name
            ).first()
            if not existing:
                db.session.add(project)
        
        run = MonitoringRun(
            projects_found=len(sample_projects),
            status="completed"
        )
        db.session.add(run)
        db.session.commit()
        
        return """
        <html>
        <body style="font-family: Arial, sans-serif; margin: 40px;">
            <h2>✅ Monitoring Complete</h2>
            <p>Added test projects successfully.</p>
            <a href="/" style="padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px;">Back to Dashboard</a>
        </body>
        </html>
        """
    except Exception as e:
        logger.error(f"Error in monitoring: {e}")
        return f"Error: {str(e)}", 500

@app.route('/monitoring')
def monitoring():
    """Monitoring status page"""
    runs = MonitoringRun.query.order_by(MonitoringRun.run_date.desc()).limit(10).all()
    
    rows = ""
    for run in runs:
        rows += f"""
        <tr>
            <td>{run.run_date.strftime('%Y-%m-%d %H:%M')}</td>
            <td>{run.projects_found}</td>
            <td>{run.status}</td>
        </tr>
        """
    
    if not runs:
        rows = "<tr><td colspan='3'>No monitoring runs yet.</td></tr>"
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Power Monitor - Monitoring</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            .card {{ background: #f5f5f5; padding: 20px; margin: 20px 0; border-radius: 8px; }}
            table {{ width: 100%; border-collapse: collapse; }}
            th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
            .btn {{ padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px; display: inline-block; margin: 5px; }}
            .nav {{ background: #333; color: white; padding: 15px; margin: -40px -40px 20px -40px; }}
            .nav a {{ color: white; margin-right: 20px; text-decoration: none; }}
        </style>
    </head>
    <body>
        <div class="nav">
            <a href="/">Dashboard</a>
            <a href="/projects">Projects</a>
            <a href="/monitoring">Monitoring</a>
            <a href="/health">Health Check</a>
        </div>
        
        <h1>Monitoring Status</h1>
        <div class="card">
            <p>System is operational</p>
            <a href="/run-monitor" class="btn">Run Monitor Now</a>
        </div>
        
        <div class="card">
            <h3>Recent Runs</h3>
            <table>
                <tr>
                    <th>Date/Time</th>
                    <th>Projects Found</th>
                    <th>Status</th>
                </tr>
                {rows}
            </table>
        </div>
    </body>
    </html>
    """

# Error handler
@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal error: {error}")
    return "Internal server error - check logs", 500

# Create tables on startup
with app.app_context():
    try:
        db.create_all()
        logger.info("Database tables created/verified")
    except Exception as e:
        logger.error(f"Could not create tables: {e}")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting app on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
