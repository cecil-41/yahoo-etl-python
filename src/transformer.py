"""
Transformer Module
Processes and transforms stock data for analysis.
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)


class StockDataTransformer:
    """Transforms stock data by calculating averages and applying filters."""
    
    def __init__(self, output_dir: str = "data/processed"):
        """
        Initialize the transformer.
        
        Args:
            output_dir: Directory to save processed files
        """
        # Get project root (parent of src directory)
        project_root = Path(__file__).parent.parent
        self.output_dir = project_root / output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_data(self, filepath: Path) -> pd.DataFrame:
        """
        Load stock data from CSV file.
        
        Args:
            filepath: Path to the CSV file
            
        Returns:
            DataFrame with stock data
        """
        logger.info(f"Loading data from {filepath}")
        
        df = pd.read_csv(filepath)
        logger.info(f"Loaded {len(df)} rows")
        
        # Convert Date column to datetime (handle timezone issues from yfinance)
        df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
        
        return df
    
    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values in the dataset.
        
        Strategy: Forward fill for price data, drop rows with missing critical values.
        Rationale: 
        - Forward fill maintains continuity in price data (assumes price carries forward)
        - Drop rows where Date or Volume is missing (cannot be meaningfully imputed)
        
        Args:
            df: DataFrame with potential missing values
            
        Returns:
            DataFrame with missing values handled
        """
        logger.info("Handling missing values...")
        
        original_count = len(df)
        
        # Log missing value counts
        missing_counts = df.isnull().sum()
        if missing_counts.any():
            logger.warning(f"Missing values detected:\n{missing_counts[missing_counts > 0]}")
        
        # Drop rows where Date or Volume is missing (critical fields)
        df = df.dropna(subset=['Date', 'Volume'])
        
        # Forward fill price columns (Close, Open, High, Low, Adj Close)
        price_columns = ['Open', 'High', 'Low', 'Close', 'Adj Close']
        for col in price_columns:
            if col in df.columns:
                df[col] = df[col].ffill()  # Updated syntax for pandas 2.0+
        
        # Drop any remaining rows with missing values
        df = df.dropna()
        
        rows_removed = original_count - len(df)
        if rows_removed > 0:
            logger.info(f"Removed {rows_removed} rows with missing values")
        else:
            logger.info("No missing values found")
        
        return df
    
    def filter_by_volume(
        self, 
        df: pd.DataFrame, 
        min_volume: int = 1_000_000
    ) -> pd.DataFrame:
        """
        Filter records where trading volume is below the threshold.
        Uses list comprehension for efficient filtering.
        
        Args:
            df: DataFrame with stock data
            min_volume: Minimum volume threshold
            
        Returns:
            Filtered DataFrame
        """
        logger.info(f"Filtering records with volume < {min_volume:,}")
        
        original_count = len(df)
        
        # Use list comprehension to create boolean mask
        mask = [volume >= min_volume for volume in df['Volume']]
        df_filtered = df[mask].copy()
        
        filtered_count = original_count - len(df_filtered)
        logger.info(f"Filtered out {filtered_count} rows ({filtered_count/original_count*100:.1f}%)")
        
        return df_filtered
    
    def calculate_yearly_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate average closing price for each year.
        
        Args:
            df: DataFrame with stock data
            
        Returns:
            DataFrame with yearly averages
        """
        logger.info("Calculating yearly average closing prices...")
        
        # Extract year from Date column
        df['Year'] = df['Date'].dt.year
        
        # Calculate average closing price per year
        yearly_avg = df.groupby('Year').agg({
            'Close': 'mean',
            'Volume': 'sum',
            'Date': 'count'
        }).round(2)
        
        # Rename columns for clarity
        yearly_avg.columns = ['Avg_Close', 'Total_Volume', 'Trading_Days']
        yearly_avg = yearly_avg.reset_index()
        
        logger.info(f"Calculated averages for {len(yearly_avg)} years")
        
        return yearly_avg
    
    def transform(self, input_filepath: Path) -> Dict[str, Any]:
        """
        Execute the complete transformation pipeline.
        
        Args:
            input_filepath: Path to the raw CSV file
            
        Returns:
            Dictionary containing transformed data and metadata
        """
        # Load data
        df = self.load_data(input_filepath)
        
        # Handle missing values
        df = self.handle_missing_values(df)
        
        # Filter by volume (before calculating averages)
        df = self.filter_by_volume(df, min_volume=1_000_000)
        
        # Calculate yearly averages
        yearly_avg = self.calculate_yearly_averages(df)
        
        # Save processed data
        output_filename = f"processed_{input_filepath.stem}.csv"
        output_filepath = self.output_dir / output_filename
        yearly_avg.to_csv(output_filepath, index=False)
        logger.info(f"Saved processed data to {output_filepath}")
        
        # Prepare result dictionary
        result = {
            'data': yearly_avg.to_dict(orient='records'),
            'metadata': {
                'source_file': str(input_filepath),
                'output_file': str(output_filepath),
                'total_years': len(yearly_avg),
                'date_range': {
                    'start': df['Date'].min().strftime('%Y-%m-%d'),
                    'end': df['Date'].max().strftime('%Y-%m-%d')
                },
                'records_processed': len(df)
            }
        }
        
        return result


if __name__ == "__main__":
    # Configure logging for standalone testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Test with a sample file
    transformer = StockDataTransformer()
    # Note: Requires a CSV file to exist
    # result = transformer.transform(Path("data/raw/AAPL_20240224_120000.csv"))
    # print(result)
