"""
Loader Module
Sends transformed data to external endpoints via HTTP POST.
"""

import logging
import requests
import json
from typing import Dict, Any

logger = logging.getLogger(__name__)


class DataLoader:
    """Loads transformed data to external endpoints."""
    
    def __init__(self, webhook_url: str):
        """
        Initialize the loader.
        
        Args:
            webhook_url: Target webhook URL for POST requests
        """
        self.webhook_url = webhook_url
    
    def send_data(self, data: Dict[str, Any]) -> requests.Response:
        """
        Send data as JSON payload via HTTP POST.
        
        Args:
            data: Dictionary containing the data to send
            
        Returns:
            Response object from the POST request
            
        Raises:
            requests.RequestException: If the POST request fails
        """
        logger.info(f"Sending data to {self.webhook_url}")
        logger.debug(f"Payload: {json.dumps(data, indent=2)}")
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'YahooFinanceETL/1.0'
        }
        
        try:
            response = requests.post(
                self.webhook_url,
                json=data,
                headers=headers,
                timeout=30
            )
            
            response.raise_for_status()
            
            logger.info(f"Successfully sent data. Status code: {response.status_code}")
            logger.debug(f"Response: {response.text[:200]}")
            
            return response
            
        except requests.RequestException as e:
            logger.error(f"Failed to send data: {e}")
            raise
    
    def save_to_file(self, data: Dict[str, Any], output_path: str = "data/processed/output.json") -> bool:
        """
        Save data to a local JSON file as an alternative to webhook.
        
        Args:
            data: Dictionary containing the data to save
            output_path: Path to save the JSON file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            from pathlib import Path
            
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Data saved to {output_file}")
            logger.info(f"Records saved: {len(data.get('data', []))}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to save file: {e}")
            return False
    
    def load(self, transformed_data: Dict[str, Any], use_file: bool = False) -> bool:
        """
        Load transformed data to the webhook endpoint or save to file.
        
        Args:
            transformed_data: Dictionary containing transformed data and metadata
            use_file: If True, save to file instead of posting to webhook
            
        Returns:
            True if successful, False otherwise
        """
        if use_file or self.webhook_url == "https://webhook.site/your-unique-url":
            logger.info("Using file output mode (webhook not configured or use_file=True)")
            return self.save_to_file(transformed_data)
        
        try:
            response = self.send_data(transformed_data)
            
            # Log success metrics
            logger.info(f"Load completed successfully")
            logger.info(f"Records sent: {len(transformed_data.get('data', []))}")
            
            return True
            
        except Exception as e:
            logger.error(f"Load failed: {e}")
            logger.info("Falling back to file output")
            return self.save_to_file(transformed_data)
            return False