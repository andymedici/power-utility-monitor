import time
import schedule
import logging
import signal
import sys
from datetime import datetime

# Import the app and the scanner function from your robust app.py
from app import app, run_full_scan

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [WORKER] - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def job():
    """Wrapper to run the scan within the Flask app context"""
    logger.info("⏰ Scheduled Job Triggered: Starting Full Scan...")
    try:
        # We must push the app context to access the database
        with app.app_context():
            run_full_scan()
        logger.info("✅ Scheduled Job Completed Successfully.")
    except Exception as e:
        logger.error(f"❌ Critical Job Failure: {e}", exc_info=True)

def graceful_shutdown(signum, frame):
    """Handle shutdown signals (like when you redeploy)"""
    logger.info("🛑 Received shutdown signal. Exiting worker...")
    sys.exit(0)

# --- Schedule Setup ---
# Run every 6 hours (matches the cadence of most ISO queue updates)
schedule.every(6).hours.do(job)

# Also run at a specific time (e.g., 8:00 AM UTC) to ensure fresh morning data
schedule.every().day.at("08:00").do(job)

if __name__ == "__main__":
    # Register signal handlers for graceful shutdown on Railway/Heroku
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    logger.info("🚀 Data Center Hunter Worker Started")
    logger.info("📅 Schedule: Every 6 hours + Daily at 08:00 UTC")

    # Run once immediately on startup so we don't have empty data
    logger.info("⚡ Running initial startup scan...")
    job()

    # Main Loop
    while True:
        try:
            schedule.run_pending()
            time.sleep(60) # Check every minute
        except KeyboardInterrupt:
            graceful_shutdown(None, None)
        except Exception as e:
            logger.error(f"⚠️ Unexpected Scheduler Error: {e}")
            time.sleep(60) # Wait before retrying to avoid tight loops
