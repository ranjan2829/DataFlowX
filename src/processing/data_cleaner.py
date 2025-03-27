import pandas as pd
import numpy as np
import re
import logging

class DataCleaner:
    """Component for cleaning and transforming data."""
    
    def __init__(self):
        """Initialize the data cleaner."""
        self.logger = logging.getLogger(__name__)
    
    def clean_dataframe(self, df, config):
        """
        Clean a pandas DataFrame based on configuration.
        
        Args:
            df: Pandas DataFrame to clean
            config: Dict with cleaning configurations
        
        Returns:
            Cleaned pandas DataFrame
        """
        original_shape = df.shape
        self.logger.info(f"Starting data cleaning on DataFrame with shape {original_shape}")
        
        # Handle missing values
        if "missing_values" in config:
            for col, strategy in config["missing_values"].items():
                if col in df.columns:
                    if strategy == "drop":
                        df = df.dropna(subset=[col])
                    elif strategy == "mean":
                        df[col] = df[col].fillna(df[col].mean())
                    elif strategy == "median":
                        df[col] = df[col].fillna(df[col].median())
                    elif strategy == "mode":
                        df[col] = df[col].fillna(df[col].mode()[0])
                    elif strategy == "zero":
                        df[col] = df[col].fillna(0)
                    elif strategy == "empty_string":
                        df[col] = df[col].fillna('')
        
        # Remove duplicates
        if config.get("remove_duplicates", False):
            df = df.drop_duplicates()
        
        # Type conversions
        if "type_conversions" in config:
            for col, new_type in config["type_conversions"].items():
                if col in df.columns:
                    try:
                        if new_type == "int":
                            df[col] = df[col].astype(int)
                        elif new_type == "float":
                            df[col] = df[col].astype(float)
                        elif new_type == "string":
                            df[col] = df[col].astype(str)
                        elif new_type == "date":
                            df[col] = pd.to_datetime(df[col])
                    except Exception as e:
                        self.logger.warning(f"Type conversion failed for column {col}: {e}")
        
        # Custom transformations
        if "custom_transforms" in config:
            for col, transform in config["custom_transforms"].items():
                if col in df.columns:
                    if transform == "lowercase":
                        df[col] = df[col].str.lower()
                    elif transform == "uppercase":
                        df[col] = df[col].str.upper()
                    elif transform == "strip":
                        df[col] = df[col].str.strip()
        
        new_shape = df.shape
        rows_removed = original_shape[0] - new_shape[0]
        self.logger.info(f"Data cleaning complete. Removed {rows_removed} rows. New shape: {new_shape}")
        
        return df
    
    @staticmethod
    def standardize_column_names(df):
        """Standardize column names (lowercase, replace spaces with underscores)."""
        df.columns = [re.sub(r'[^\w\s]', '', col).lower().replace(' ', '_') for col in df.columns]
        return df