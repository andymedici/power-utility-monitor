# run_monitor.py - Simple Scheduler for Railway
import os
import sys
import time
import logging
from datetime import datetime

# Fix Railway PostgreSQL URL
if 'DATABASE_URL' in os.environ:
    if os.environ['DATABASE_URL'].startswith('postgres://'):
        os.environ['DATABASE_URL'] = os.environ['DATABASE_URL'].replace('postgres://', 'postgresql://', 1)
        print("Fixed DATABASE_URL format for scheduler", flush=True)

# Import the app
from app import app, db, PowerProject, MonitoringRun

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_monitoring():
    """Simple monitoring function"""
    try:
        logger.info(f"Running monitoring at {datetime.now()}")
        
        with app.app_context():
            # Add test data (replace with real monitoring later)
            test_project = PowerProject(
                project_name=f"Test Project {datetime.now().strftime('%H%M')}",
                capacity_mw=200.0,
                location="Test Location",
                state="VA",
                source="Scheduler Test"
            )
            db.session.add(test_project)
            
            run = MonitoringRun(
                projects_found=1,
                status="completed"
            )
            db.session.add(run)
            db.session.commit()
            
            logger.info("Monitoring completed successfully")
            
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")

# Run once on startup
logger.info("Scheduler starting...")
run_monitoring()

# Run every hour (simpler than daily for testing)
logger.info("Scheduler running - will check every hour")
while True:
    time.sleep(3600)  # Sleep for 1 hour
    run_monitoring()
