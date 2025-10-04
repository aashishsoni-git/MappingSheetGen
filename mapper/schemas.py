# mapper/schemas.py - SIMPLEST VERSION THAT WORKS
from pydantic import BaseModel, Field, ConfigDict
from typing import List


class ColumnMapping(BaseModel):
    """Represents a single mapping from source XML to target Silver column"""
    model_config = ConfigDict(extra='forbid')
    
    source_node: str = Field(description="Full XPath of source XML node")
    target_table: str = Field(description="Target Silver table name")
    target_column: str = Field(description="Target column name")
    transformation_logic: str = Field(description="SQL transformation or empty string")  # Required, not optional
    confidence_score: float = Field(ge=0.0, le=1.0, description="Confidence 0-1")
    reasoning: str = Field(description="Brief explanation")


class MappingPrediction(BaseModel):
    """Collection of all predicted mappings"""
    model_config = ConfigDict(extra='forbid')
    
    mappings: List[ColumnMapping] = Field(description="List of mappings")
