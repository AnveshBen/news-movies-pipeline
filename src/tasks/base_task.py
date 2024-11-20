from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from src.utils.storage_manager import StorageManager
import logging

class BaseAnalysisOperator(BaseOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.storage = StorageManager()
        
    def pre_execute(self, context):
        """Validate prerequisites before execution"""
        self.validate_prerequisites()
        
    def post_execute(self, context, result=None):
        """Cleanup after execution"""
        self.cleanup()
        
    def validate_prerequisites(self):
        """Validate task prerequisites"""
        raise NotImplementedError
        
    def cleanup(self):
        """Cleanup resources"""
        pass 