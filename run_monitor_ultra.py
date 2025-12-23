import time
import schedule
import logging
import signal
import sys
from datetime import datetime
from app_complete import app, run_full_scan

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [WORKER] - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def job():
    """Wrapper to run the scan within the Flask app context"""
    logger.info("⏰ Scheduled Job Triggered: Starting Full Scan...")
    try:
        with app.app_context():
            run_full_scan()
        logger.info("✅ Scheduled Job Completed Successfully.")
    except Exception as e:
        logger.error(f"❌ Critical Job Failure: {e}", exc_info=True)

def graceful_shutdown(signum, frame):
    """Handle shutdown signals"""
    logger.info("🛑 Received shutdown signal. Exiting worker...")
    sys.exit(0)

# Schedule: Every 6 hours + Daily at 8 AM UTC
schedule.every(6).hours.do(job)
schedule.every().day.at("08:00").do(job)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)
    
    logger.info("🚀 Ultra Power Monitor Worker Started")
    logger.info("📅 Schedule: Every 6 hours + Daily at 08:00 UTC")
    
    # Run once on startup
    logger.info("⚡ Running initial startup scan...")
    job()
    
    # Main loop
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)
        except KeyboardInterrupt:
            graceful_shutdown(None, None)
        except Exception as e:
            logger.error(f"⚠️ Unexpected Scheduler Error: {e}")
            time.sleep(60)
