import tensorflow as tf
import pandas as pd
import numpy as np
import pickle
import json
import logging
import os
from typing import Dict, List, Any, Union

class ModelService:
    """Service for ML model operations and inference."""
    
    def __init__(self, models_dir='models'):
        """Initialize model service."""
        self.models_dir = models_dir
        self.models = {}
        self.logger = logging.getLogger(__name__)
        
        # Create models directory if it doesn't exist
        os.makedirs(models_dir, exist_ok=True)
    
    def load_model(self, model_name: str, model_type: str = 'tensorflow') -> bool:
        """
        Load a machine learning model.
        
        Args:
            model_name: Name of the model to load
            model_type: Type of model (tensorflow, sklearn, pickle)
            
        Returns:
            bool: Success status
        """
        try:
            model_path = os.path.join(self.models_dir, model_name)
            
            if model_type == 'tensorflow':
                self.models[model_name] = tf.keras.models.load_model(model_path)
            elif model_type == 'sklearn' or model_type == 'pickle':
                with open(model_path, 'rb') as f:
                    self.models[model_name] = pickle.load(f)
            else:
                self.logger.error(f"Unknown model type: {model_type}")
                return False
                
            self.logger.info(f"Successfully loaded model: {model_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load model {model_name}: {e}")
            return False
    
    def predict(self, model_name: str, data: Union[pd.DataFrame, np.ndarray, List]) -> Dict[str, Any]:
        """
        Make predictions using the specified model.
        
        Args:
            model_name: Name of the model to use
            data: Input data for prediction
            
        Returns:
            Dict with prediction results and metadata
        """
        if model_name not in self.models:
            self.logger.error(f"Model {model_name} not loaded")
            return {"error": f"Model {model_name} not loaded"}
        
        try:
            # Make prediction
            if isinstance(data, pd.DataFrame):
                results = self.models[model_name].predict(data.values)
            else:
                results = self.models[model_name].predict(data)
            
            # Convert numpy types to native Python types for JSON serialization
            if isinstance(results, np.ndarray):
                results = results.tolist()
            
            return {
                "success": True,
                "predictions": results,
                "model_name": model_name,
                "prediction_count": len(results)
            }
            
        except Exception as e:
            self.logger.error(f"Prediction error with model {model_name}: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def batch_predict(self, model_name: str, data_generator) -> Dict[str, Any]:
        """
        Make predictions on batches of data.
        
        Args:
            model_name: Name of the model to use
            data_generator: Generator yielding batches of data
            
        Returns:
            Dict with aggregated prediction results
        """
        if model_name not in self.models:
            self.logger.error(f"Model {model_name} not loaded")
            return {"error": f"Model {model_name} not loaded"}
        
        try:
            all_predictions = []
            batch_count = 0
            
            for batch in data_generator:
                batch_predictions = self.models[model_name].predict(batch)
                all_predictions.extend(batch_predictions.tolist() if isinstance(batch_predictions, np.ndarray) else batch_predictions)
                batch_count += 1
            
            return {
                "success": True,
                "predictions": all_predictions,
                "model_name": model_name,
                "batch_count": batch_count,
                "prediction_count": len(all_predictions)
            }
            
        except Exception as e:
            self.logger.error(f"Batch prediction error with model {model_name}: {e}")
            return {
                "success": False,
                "error": str(e)
            }