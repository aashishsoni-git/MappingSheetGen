# mapper/openai_mapper.py - COMPLETE UPDATED VERSION
from typing import Dict, Any, List, Optional
from openai import OpenAI
import pandas as pd
from mapper.schemas import MappingPrediction
from utils.decorators import etl_operation, handle_openai_errors, log_execution_time
from utils.cost_estimator import CostEstimator
from utils.validators import DataValidator
import logging
import os

logger = logging.getLogger(__name__)


class AIETLMapper:
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)
        self.model = "gpt-4o-2024-08-06"
        self.cost_estimator = CostEstimator(self.model)
        self.validator = DataValidator()
    
    @etl_operation(max_retries=3)
    def predict_mappings(self, 
                        xml_metadata_df, 
                        silver_schema_df, 
                        historical_mappings_df,
                        data_dictionary_df):
        """Generate mapping predictions using OpenAI API - LEGACY METHOD"""
        
        # Validate inputs
        if not self.validator.validate_xml_metadata(xml_metadata_df):
            raise ValueError("Invalid XML metadata")
        
        # Estimate cost before API call
        cost_estimate = self.cost_estimator.estimate_cost(
            len(xml_metadata_df),
            len(silver_schema_df),
            len(historical_mappings_df) if not historical_mappings_df.empty else 0
        )
        logger.info(f"Estimated cost: ${cost_estimate['total_cost']:.4f}")
        
        # Build prompt
        prompt = self._build_prompt(
            xml_metadata_df,
            silver_schema_df,
            historical_mappings_df,
            data_dictionary_df
        )
        
        # Call OpenAI API
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are an expert ETL data mapper with deep knowledge of data warehousing and insurance domain."},
                {"role": "user", "content": prompt}
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "mapping_prediction",
                    "strict": True,
                    "schema": MappingPrediction.model_json_schema()
                }
            }
        )
        
        # Track actual cost
        actual_cost = self.cost_estimator.track_actual_usage(response)
        logger.info(f"Actual cost: ${actual_cost['total_cost']:.4f}")
        logger.info(f"Cumulative cost: ${actual_cost['cumulative_cost']:.4f}")
        
        # Parse and validate results
        mappings = MappingPrediction.model_validate_json(
            response.choices[0].message.content
        )
        
        # Validate predictions
        issues = self.validator.validate_mapping_predictions(
            [m.dict() for m in mappings.mappings]
        )
        
        if issues:
            logger.warning(f"Found {len(issues)} validation issues")
            for issue in issues:
                logger.warning(f"  - {issue['message']}")
        
        return mappings
    
    @etl_operation(max_retries=3)
    def predict_mappings_flexible(self, 
                                  xml_metadata_df: pd.DataFrame, 
                                  silver_schema_df: pd.DataFrame, 
                                  reference_data: Dict[str, Any]) -> MappingPrediction:
        """
        Generate mappings with FLEXIBLE reference data
        AI understands whatever format the reference data is in
        """
        
        # Validate inputs
        if not self.validator.validate_xml_metadata(xml_metadata_df):
            raise ValueError("Invalid XML metadata")
        
        logger.info(f"🤖 Generating mappings with flexible reference data...")
        logger.info(f"   XML nodes: {len(xml_metadata_df)}")
        logger.info(f"   Silver columns: {len(silver_schema_df)}")
        
        # Build flexible prompt based on what reference data we have
        prompt = self._build_flexible_prompt(
            xml_metadata_df,
            silver_schema_df,
            reference_data
        )
        
        # Estimate cost
        estimated_tokens = len(prompt) // 4  # Rough estimate
        logger.info(f"💰 Estimated tokens: ~{estimated_tokens:,}")
        
        # Call OpenAI API
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {
                    "role": "system", 
                    "content": "You are an expert ETL data mapper. You understand various data formats and can learn from examples in any structure."
                },
                {"role": "user", "content": prompt}
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "mapping_prediction",
                    "strict": True,
                    "schema": MappingPrediction.model_json_schema()
                }
            },
            temperature=0.3
        )
        
        # Track actual cost
        actual_cost = self.cost_estimator.track_actual_usage(response)
        logger.info(f"✅ Actual cost: ${actual_cost['total_cost']:.4f}")
        logger.info(f"📊 Cumulative cost: ${actual_cost['cumulative_cost']:.4f}")
        
        # Parse results
        mappings = MappingPrediction.model_validate_json(
            response.choices[0].message.content
        )
        
        logger.info(f"✅ Generated {len(mappings.mappings)} mappings")
        
        # Validate predictions
        issues = self.validator.validate_mapping_predictions(
            [m.dict() for m in mappings.mappings]
        )
        
        if issues:
            logger.warning(f"⚠️ Found {len(issues)} validation issues")
            for issue in issues[:5]:  # Show first 5
                logger.warning(f"  - {issue['message']}")
        
        return mappings
    
    def _build_prompt(self, xml_metadata_df, silver_schema_df, 
                     historical_mappings_df, data_dictionary_df):
        """Build prompt for OpenAI - LEGACY"""
        
        prompt = f"""You are mapping XML source data to Snowflake Silver layer tables.

## XML SOURCE METADATA ({len(xml_metadata_df)} nodes):
{xml_metadata_df[['node_path', 'node_name', 'data_type', 'sample_value']].head(50).to_string(index=False)}

## SILVER LAYER SCHEMA ({len(silver_schema_df)} columns):
{silver_schema_df[['table_name', 'column_name', 'data_type', 'description']].head(50).to_string(index=False)}
"""

        if not historical_mappings_df.empty:
            prompt += f"""
## HISTORICAL MAPPINGS ({len(historical_mappings_df)} examples):
{historical_mappings_df[['source_node', 'target_table', 'target_column', 'transformation']].head(20).to_string(index=False)}
"""

        if not data_dictionary_df.empty:
            prompt += f"""
## DATA DICTIONARY:
{data_dictionary_df[['table_name', 'column_name', 'description', 'business_rules']].head(30).to_string(index=False)}
"""

        prompt += """

## TASK:
For each Silver layer column, predict the best matching XML source node. Provide:
1. source_node: Full XPath from XML
2. target_table: Silver table name
3. target_column: Silver column name  
4. transformation_logic: SQL transformation needed (use empty string "" for direct mapping)
5. confidence_score: Float between 0 and 1
6. reasoning: Brief explanation of why this mapping makes sense

Focus on semantic meaning, data types, and business context. Use historical mappings as patterns.
Return ALL fields for EVERY mapping. Use empty string "" for transformation_logic if no transformation needed.
"""
        
        return prompt
    
    def _build_flexible_prompt(self,
                              xml_metadata_df: pd.DataFrame,
                              silver_schema_df: pd.DataFrame,
                              reference_data: Dict[str, Any]) -> str:
        """
        Build prompt with FLEXIBLE reference data
        Handles any format/structure
        """
        
        prompt = f"""You are an expert ETL data mapper creating mappings from XML source to Snowflake Silver tables.

## 📊 XML SOURCE METADATA ({len(xml_metadata_df)} nodes):
{xml_metadata_df[['node_path', 'node_name', 'data_type', 'sample_value']].head(50).to_string(index=False)}

## 🗄️ SILVER LAYER SCHEMA ({len(silver_schema_df)} columns):
{silver_schema_df[['table_name', 'column_name', 'data_type']].head(50).to_string(index=False)}
"""
        
        # Add historical mappings if available (any format/structure)
        if reference_data.get('historical_mappings'):
            prompt += "\n" + "="*80 + "\n"
            prompt += "## 📚 HISTORICAL MAPPINGS (learn from these patterns):\n"
            prompt += "="*80 + "\n"
            
            for idx, mapping_file in enumerate(reference_data['historical_mappings'][:3]):  # Max 3 files
                df = mapping_file['data']['dataframe']
                filename = mapping_file['filename']
                relevance = mapping_file['classification']['relevance_score']
                
                prompt += f"\n### Reference File {idx+1}: {filename} (Relevance: {relevance}%)\n"
                prompt += f"Columns: {df.columns.tolist()}\n"
                prompt += f"Sample data:\n{df.head(15).to_string(index=False)}\n"
        
        # Add data dictionaries if available (any format/structure)
        if reference_data.get('data_dictionaries'):
            prompt += "\n" + "="*80 + "\n"
            prompt += "## 📖 DATA DICTIONARIES (understand business meaning):\n"
            prompt += "="*80 + "\n"
            
            for idx, dict_file in enumerate(reference_data['data_dictionaries'][:2]):  # Max 2 files
                df = dict_file['data']['dataframe']
                filename = dict_file['filename']
                
                prompt += f"\n### Dictionary {idx+1}: {filename}\n"
                prompt += f"Columns: {df.columns.tolist()}\n"
                prompt += f"Sample data:\n{df.head(30).to_string(index=False)}\n"
        
        # Add schema definitions if available
        if reference_data.get('schema_definitions'):
            prompt += "\n" + "="*80 + "\n"
            prompt += "## 🗄️ SCHEMA DEFINITIONS:\n"
            prompt += "="*80 + "\n"
            
            for idx, schema_file in enumerate(reference_data['schema_definitions'][:1]):
                df = schema_file['data']['dataframe']
                filename = schema_file['filename']
                
                prompt += f"\n### Schema: {filename}\n"
                prompt += f"{df.head(20).to_string(index=False)}\n"
        
        prompt += "\n" + "="*80 + "\n"
        prompt += """
## 🎯 YOUR TASK:
Create ETL mappings from XML source nodes to Silver layer columns.

For EACH mapping, provide:
1. **source_node**: Full XPath from XML (e.g., /Session/Data/Policy/PolicyID)
2. **target_table**: Silver table name (e.g., SILVER.POLICY)
3. **target_column**: Column name (e.g., policy_id)
4. **transformation_logic**: SQL transformation or empty string "" for direct copy
5. **confidence_score**: Float 0.0-1.0 (how confident are you?)
6. **reasoning**: Brief explanation (1-2 sentences)

## 💡 MAPPING GUIDELINES:
- Match based on semantic meaning, not just name similarity
- Use historical mappings as patterns (same XML structure → same SQL transforms)
- Check data types compatibility
- Higher confidence for exact matches in historical data
- Lower confidence for inferred/guessed mappings
- Use empty string "" for transformation_logic if no transformation needed
- Consider business context from data dictionary

## ⚠️ IMPORTANT:
- Return ALL 6 fields for EVERY mapping
- Use empty string "" for transformation_logic (not null)
- Focus on most important/common columns first
- It's okay to skip very obscure columns
"""
        
        return prompt
