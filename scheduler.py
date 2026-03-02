"""
Simple Scheduler for ETL Pipeline
Runs the pipeline at specified intervals using schedule library.
"""

import schedule
import time
import subprocess
import logging
from datetime import datetime
from pathlib import Path

# Setup logging
log_dir = Path(__file__).parent / "logs"
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "scheduler.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def run_etl_pipeline():
    """Execute the ETL pipeline."""
    logger.info("=" * 80)
    logger.info(f"Starting scheduled ETL pipeline run at {datetime.now()}")
    logger.info("=" * 80)
    
    try:
        # Get the Python executable from the virtual environment
        project_root = Path(__file__).parent
        python_exe = project_root / ".venv" / "Scripts" / "python.exe"
        
        # Fallback to system python if venv not found
        if not python_exe.exists():
            python_exe = "python"
        
        # Run the main ETL pipeline
        result = subprocess.run(
            [str(python_exe), "src/main.py", "--ticker", "AAPL"],
            capture_output=True,
            text=True,
            cwd=project_root
        )
        
        if result.returncode == 0:
            logger.info("ETL pipeline completed successfully")
            logger.info(f"Output:\n{result.stdout}")
        else:
            logger.error(f"ETL pipeline failed with exit code {result.returncode}")
            logger.error(f"Error output:\n{result.stderr}")
            
    except Exception as e:
        logger.error(f"Failed to run ETL pipeline: {e}")


def main():
    """Main scheduler loop."""
    logger.info("ETL Pipeline Scheduler started")
    logger.info("Scheduling configuration:")
    logger.info("  - Daily at 16:30 (4:30 PM - after market close)")
    logger.info("  - Every Monday at 09:00 (weekly summary)")
    
    # Schedule daily run at 4:30 PM (after market close)
    schedule.every().day.at("16:30").do(run_etl_pipeline)
    
    # Schedule weekly run every Monday at 9:00 AM
    schedule.every().monday.at("09:00").do(run_etl_pipeline)
    
    
    logger.info("Scheduler is running. Press Ctrl+C to stop.")
    
    # Run immediately on startup
    run_etl_pipeline()
    
    # Keep the scheduler running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")


if __name__ == "__main__":
    main()
