import time
import schedule
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_monitoring():
    """Run the monitoring cycle"""
    try:
        logger.info(f"Starting scheduled monitoring at {datetime.now()}")
        
        # Import here to avoid circular imports
        from app import app, db, ComprehensivePowerMonitor
        
        with app.app_context():
            monitor = ComprehensivePowerMonitor()
            result = monitor.run_comprehensive_monitoring()
            logger.info(f"Monitoring complete: {result['projects_stored']} new projects stored from {result['sources_checked']} sources")
    except Exception as e:
        logger.error(f"Monitoring failed: {e}")

# Schedule daily at midnight UTC
schedule.every().day.at("00:00").do(run_monitoring)

# Run once on startup
logger.info("Running initial monitoring check...")
run_monitoring()

logger.info("Scheduler started. Will run daily at 00:00 UTC")
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute
