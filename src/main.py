"""
Main ETL Pipeline Orchestrator
Coordinates the Extract, Transform, Load process for Yahoo Finance stock data.
"""

import logging
import argparse
import yaml
from pathlib import Path
from datetime import datetime

from extractor import YahooFinanceExtractor
from transformer import StockDataTransformer
from loader import DataLoader


def setup_logging(log_level: str = "INFO"):
    """
    Configure logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    # Get project root (parent of src directory)
    project_root = Path(__file__).parent.parent
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"etl_pipeline_{timestamp}.log"
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    
    return logger


def load_config(config_path: str = "config/config.yaml") -> dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Configuration dictionary
    """
    # Get project root (parent of src directory)
    project_root = Path(__file__).parent.parent
    config_file = project_root / config_path
    
    if config_file.exists():
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    else:
        # Return default configuration
        return {
            'webhook_url': 'https://webhook.site/your-unique-url',
            'ticker': 'AAPL',
            'min_volume': 1000000
        }


def run_etl_pipeline(
    ticker: str,
    webhook_url: str,
    min_volume: int = 1_000_000,
    use_file_output: bool = False
) -> bool:
    """
    Execute the complete ETL pipeline.
    
    Args:
        ticker: Stock ticker symbol
        webhook_url: Webhook endpoint URL
        min_volume: Minimum trading volume filter
        use_file_output: If True, save to JSON file instead of webhook
        
    Returns:
        True if pipeline succeeds, False otherwise
    """
    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("Starting ETL Pipeline")
    logger.info(f"Ticker: {ticker}")
    logger.info(f"Webhook URL: {webhook_url}")
    logger.info(f"Output Mode: {'File' if use_file_output else 'Webhook'}")
    logger.info(f"Min Volume Filter: {min_volume:,}")
    logger.info("=" * 80)
    
    try:
        # EXTRACT
        logger.info("STEP 1: EXTRACT")
        extractor = YahooFinanceExtractor()
        raw_data_path = extractor.extract(ticker)
        logger.info(f"✓ Extract completed: {raw_data_path}")
        
        # TRANSFORM
        logger.info("\nSTEP 2: TRANSFORM")
        transformer = StockDataTransformer()
        transformed_data = transformer.transform(raw_data_path)
        logger.info(f"✓ Transform completed: {len(transformed_data['data'])} years processed")
        
        # Display summary
        logger.info("\nTransformed Data Summary:")
        for record in transformed_data['data']:
            logger.info(f"  Year {record['Year']}: "
                       f"Avg Close = ${record['Avg_Close']:.2f}, "
                       f"Trading Days = {record['Trading_Days']}")
        
        # LOAD
        logger.info("\nSTEP 3: LOAD")
        loader = DataLoader(webhook_url)
        success = loader.load(transformed_data, use_file=use_file_output)
        
        if success:
            logger.info("✓ Load completed successfully")
        else:
            logger.error("✗ Load failed")
            return False
        
        logger.info("=" * 80)
        logger.info("ETL Pipeline completed successfully!")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
        return False


def main():
    """Main entry point for the ETL pipeline."""
    parser = argparse.ArgumentParser(
        description='Yahoo Finance Stock Data ETL Pipeline'
    )
    parser.add_argument(
        '--ticker',
        type=str,
        default='AAPL',
        help='Stock ticker symbol (default: AAPL)'
    )
    parser.add_argument(
        '--webhook-url',
        type=str,
        help='Webhook URL for posting data (overrides config)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Path to configuration file (default: config/config.yaml)'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    parser.add_argument(
        '--min-volume',
        type=int,
        default=1_000_000,
        help='Minimum trading volume filter (default: 1,000,000)'
    )
    parser.add_argument(
        '--use-file',
        action='store_true',
        help='Save output to JSON file instead of webhook (useful for corporate networks)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.log_level)
    
    # Load configuration
    config = load_config(args.config)
    
    # Command-line arguments override config
    ticker = args.ticker or config.get('ticker', 'AAPL')
    webhook_url = args.webhook_url or config.get('webhook_url')
    min_volume = args.min_volume or config.get('min_volume', 1_000_000)
    use_file_output = args.use_file
    
    if not webhook_url and not use_file_output:
        logger.error("Webhook URL not provided. Use --webhook-url or --use-file flag")
        return 1
    
    # Run ETL pipeline
    success = run_etl_pipeline(ticker, webhook_url or "", min_volume, use_file_output)
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
