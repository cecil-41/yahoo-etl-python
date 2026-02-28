"""
Extractor Module
Downloads historical stock price data from Yahoo Finance using yfinance library.
"""

import logging
import yfinance as yf
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


class YahooFinanceExtractor:
    """Extracts historical stock data from Yahoo Finance."""
    
    def __init__(self, output_dir: str = "data/raw"):
        """
        Initialize the extractor.
        
        Args:
            output_dir: Directory to save downloaded CSV files
        """
        # Get project root (parent of src directory)
        project_root = Path(__file__).parent.parent
        self.output_dir = project_root / output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def download_stock_data(
        self, 
        ticker: str, 
        start_date: str = "2022-01-01",
        end_date: str = "2025-12-31"
    ) -> Path:
        """
        Download historical stock data from Yahoo Finance using yfinance library.
        
        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Path to the downloaded CSV file
            
        Raises:
            Exception: If download fails
        """
        logger.info(f"Downloading {ticker} data from Yahoo Finance using yfinance...")
        logger.debug(f"Date range: {start_date} to {end_date}")
        
        try:
            # Download data using yfinance
            stock = yf.Ticker(ticker)
            df = stock.history(start=start_date, end=end_date)
            
            if df.empty:
                raise ValueError(f"No data retrieved for {ticker}")
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{ticker}_{timestamp}.csv"
            filepath = self.output_dir / filename
            
            # Save CSV data
            df.to_csv(filepath)
            
            logger.info(f"Successfully downloaded {len(df)} rows to {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Failed to download data: {e}")
            raise
