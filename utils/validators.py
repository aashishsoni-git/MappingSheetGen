# utils/validators.py
"""
Data validation utilities for ETL mapping
"""
import pandas as pd
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class DataValidator:
    """Validate data quality and mapping predictions"""
    
    @staticmethod
    def validate_xml_metadata(df: pd.DataFrame) -> bool:
        """Validate XML metadata DataFrame structure"""
        required_columns = ['node_path', 'node_name', 'data_type']
        
        if not all(col in df.columns for col in required_columns):
            missing = [col for col in required_columns if col not in df.columns]
            logger.error(f"Missing required columns in XML metadata: {missing}")
            return False
        
        if df.empty:
            logger.error("XML metadata is empty")
            return False
        
        logger.info(f"XML metadata validation passed: {len(df)} nodes")
        return True
    
    @staticmethod
    def validate_mapping_predictions(predictions: List[Dict]) -> List[Dict]:
        """
        Validate and flag low-confidence predictions
        
        Returns:
            List of validation issues
        """
        issues = []
        
        for idx, mapping in enumerate(predictions):
            # Check confidence threshold
            if mapping.get('confidence_score', 0) < 0.5:
                issues.append({
                    'mapping_index': idx,
                    'issue_type': 'low_confidence',
                    'message': f"Low confidence ({mapping['confidence_score']:.2f}) for "
                              f"{mapping['source_node']} → {mapping['target_column']}",
                    'severity': 'warning'
                })
            
            # Check for missing transformation logic where types don't match
            source_type = mapping.get('source_data_type', '').lower()
            target_type = mapping.get('target_data_type', '').lower()
            
            if source_type != target_type and not mapping.get('transformation_logic'):
                issues.append({
                    'mapping_index': idx,
                    'issue_type': 'type_mismatch',
                    'message': f"Type mismatch without transformation: "
                              f"{source_type} → {target_type}",
                    'severity': 'error'
                })
        
        return issues
    
    @staticmethod
    def check_data_type_compatibility(source_type: str, target_type: str) -> bool:
        """Check if data types are compatible"""
        type_compatibility = {
            'string': ['varchar', 'text', 'string', 'char'],
            'integer': ['int', 'integer', 'bigint', 'smallint', 'number'],
            'decimal': ['float', 'double', 'decimal', 'numeric', 'number'],
            'date': ['date', 'timestamp', 'datetime']
        }
        
        for group in type_compatibility.values():
            if source_type.lower() in group and target_type.lower() in group:
                return True
        
        return False
