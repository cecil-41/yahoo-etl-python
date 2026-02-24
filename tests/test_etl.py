"""
Unit tests for ETL pipeline components
"""

import pytest
import pandas as pd
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transformer import StockDataTransformer


class TestTransformer:
    """Test cases for the StockDataTransformer class."""
    
    def test_handle_missing_values_forward_fill(self):
        """Test that missing price values are forward filled."""
        transformer = StockDataTransformer()
        
        # Create sample data with missing values
        data = {
            'Date': pd.date_range('2021-01-01', periods=5),
            'Close': [100, None, None, 105, 110],
            'Volume': [1000000, 1100000, 1200000, 1300000, 1400000]
        }
        df = pd.DataFrame(data)
        
        result = transformer.handle_missing_values(df)
        
        # Check that missing values are filled
        assert result['Close'].isna().sum() == 0
        assert result['Close'].iloc[1] == 100  # Forward filled
        assert result['Close'].iloc[2] == 100  # Forward filled
    
    def test_filter_by_volume(self):
        """Test volume filtering with list comprehension."""
        transformer = StockDataTransformer()
        
        data = {
            'Date': pd.date_range('2021-01-01', periods=5),
            'Close': [100, 101, 102, 103, 104],
            'Volume': [500000, 1500000, 800000, 2000000, 1200000]
        }
        df = pd.DataFrame(data)
        
        result = transformer.filter_by_volume(df, min_volume=1_000_000)
        
        # Should keep only rows with volume >= 1,000,000
        assert len(result) == 3
        assert all(result['Volume'] >= 1_000_000)
    
    def test_calculate_yearly_averages(self):
        """Test yearly average calculation."""
        transformer = StockDataTransformer()
        
        data = {
            'Date': pd.to_datetime(['2021-01-01', '2021-06-01', '2022-01-01', '2022-06-01']),
            'Close': [100, 110, 120, 130],
            'Volume': [1000000, 1100000, 1200000, 1300000]
        }
        df = pd.DataFrame(data)
        
        result = transformer.calculate_yearly_averages(df)
        
        # Should have 2 years
        assert len(result) == 2
        assert 2021 in result['Year'].values
        assert 2022 in result['Year'].values
        
        # Check average for 2021
        avg_2021 = result[result['Year'] == 2021]['Avg_Close'].iloc[0]
        assert avg_2021 == 105.0  # (100 + 110) / 2


class TestIntegration:
    """Integration tests for the full pipeline."""
    
    def test_data_directories_exist(self):
        """Test that required directories are created."""
        assert Path("data/raw").exists()
        assert Path("data/processed").exists()
        assert Path("logs").exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
