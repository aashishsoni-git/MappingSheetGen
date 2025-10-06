"""
OpenAI-powered mapping generator - WORKS WITH ANY GPT MODEL
"""
from openai import OpenAI
from typing import List, Dict, Any
import logging
import snowflake.connector
import pandas as pd
import json
import re

try:
    from .schemas import ETLMappingResult, ETLMapping
except ImportError:
    from dataclasses import dataclass
    from typing import Optional
    
    @dataclass
    class ETLMapping:
        source_node: str
        target_table: str
        target_column: str
        transformation_logic: Optional[str]
        confidence_score: float
        reasoning: str
    
    @dataclass
    class ETLMappingResult:
        source_file: str
        product_code: str
        mappings: List[ETLMapping]
        total_mappings: int

logger = logging.getLogger(__name__)


class OpenAIMapper:
    
    def __init__(self, api_key: str, model: str = "gpt-4", snowflake_config: Dict = None):
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.sf_config = snowflake_config
        self.available_tables = self._fetch_silver_tables()
    
    def _fetch_silver_tables(self) -> Dict[str, str]:
        if not self.sf_config:
            return {'POLICY': 'SILVER.POLICY', 'ACCOUNT': 'SILVER.ACCOUNT', 'RISK': 'SILVER.RISK', 
                    'COVERAGE': 'SILVER.COVERAGE', 'PAYMENT': 'SILVER.PAYMENT', 'CUSTOMER': 'SILVER.CUSTOMER', 'QUOTE': 'SILVER.QUOTE'}
        try:
            conn = snowflake.connector.connect(**self.sf_config)
            cursor = conn.cursor()
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SILVER'")
            tables = {row[0]: f"SILVER.{row[0]}" for row in cursor.fetchall()}
            cursor.close()
            conn.close()
            return tables if tables else self._get_default_tables()
        except:
            return self._get_default_tables()
    
    def _get_default_tables(self) -> Dict[str, str]:
        return {'POLICY': 'SILVER.POLICY', 'ACCOUNT': 'SILVER.ACCOUNT', 'RISK': 'SILVER.RISK', 
                'COVERAGE': 'SILVER.COVERAGE', 'PAYMENT': 'SILVER.PAYMENT', 'CUSTOMER': 'SILVER.CUSTOMER', 'QUOTE': 'SILVER.QUOTE'}
    
    def predict_mappings_flexible(self, xml_metadata: Any, silver_schema: Any, reference_data: Dict[str, Any]) -> ETLMappingResult:
        if isinstance(silver_schema, pd.DataFrame):
            schema_dict = self._transform_schema_dataframe(silver_schema)
        else:
            schema_dict = silver_schema
        
        if isinstance(xml_metadata, pd.DataFrame):
            nodes = [{'xpath': row.get('node_path', row.get('xpath', '')), 'data_type': row.get('data_type', 'string'), 
                     'sample_value': row.get('sample_value', '')} for _, row in xml_metadata.iterrows()]
            xml_data = {'nodes': nodes}
        else:
            xml_data = xml_metadata
        
        return self.generate_mappings(xml_data, schema_dict, reference_data)
    
    def _transform_schema_dataframe(self, df: pd.DataFrame) -> Dict[str, List[Dict]]:
        schema_dict = {}
        if 'table_name' in df.columns:
            for table_name, group in df.groupby('table_name'):
                columns = [{'column_name': r.get('column_name', ''), 'data_type': r.get('data_type', '')} for _, r in group.iterrows()]
                schema_dict[table_name] = columns
                schema_dict[table_name.upper()] = columns
                schema_dict[f"SILVER.{table_name}"] = columns
                schema_dict[f"SILVER.{table_name.upper()}"] = columns
        return schema_dict
    
    def generate_mappings(self, xml_data: Dict[str, Any], silver_schema: Dict[str, List[Dict]], reference_data: Dict[str, Any]) -> ETLMappingResult:
        nodes = xml_data.get('nodes', [])
        if not nodes:
            return ETLMappingResult(source_file='unknown.xml', product_code='UNKNOWN', mappings=[], total_mappings=0)
        
        nodes_by_section = self._group_nodes_by_section(nodes)
        all_mappings = []
        
        for section_name, section_nodes in nodes_by_section.items():
            target_table = self._determine_target_table(section_name)
            section_mappings = self._generate_section_mappings(section_nodes, target_table, silver_schema)
            all_mappings.extend(section_mappings)
        
        return ETLMappingResult(source_file='unknown.xml', product_code='UNKNOWN', mappings=all_mappings, total_mappings=len(all_mappings))
    
    def _group_nodes_by_section(self, nodes: List[Dict]) -> Dict[str, List[Dict]]:
        sections = {}
        for node in nodes:
            path = node.get('xpath', '')
            if not path:
                continue
            section = None
            for part in path.split('/'):
                if any(kw in part.lower() for kw in ['policy', 'account', 'risk', 'coverage', 'payment', 'customer', 'quote']):
                    section = part
                    break
            sections.setdefault(section or 'General', []).append(node)
        return sections
    
    def _determine_target_table(self, section_name: str) -> str:
        section_lower = section_name.lower()
        routing = {'policy': 'POLICY', 'account': 'ACCOUNT', 'risk': 'RISK', 'coverage': 'COVERAGE', 
                  'payment': 'PAYMENT', 'customer': 'CUSTOMER', 'quote': 'QUOTE'}
        for keyword, table in routing.items():
            if keyword in section_lower and table in self.available_tables:
                return self.available_tables[table]
        return list(self.available_tables.values())[0] if self.available_tables else 'SILVER.POLICY'
    
    def _generate_section_mappings(self, nodes: List[Dict], target_table: str, silver_schema: Dict) -> List[ETLMapping]:
        table_name = target_table.split('.')[-1]
        target_columns = None
        for key in [table_name, target_table, f"SILVER.{table_name}"]:
            if key in silver_schema:
                target_columns = silver_schema[key]
                break
        
        if not target_columns:
            return []
        
        nodes_sum = chr(10).join([f"- {n.get('xpath', 'unknown')}" for n in nodes[:20]])
        cols_sum = chr(10).join([f"- {c['column_name']}" for c in target_columns[:20]])
        
        prompt = f"""Map XML to {target_table}. Return JSON array only.

XML: {nodes_sum}

COLS: {cols_sum}

Format: [{{"source_node":"/path","target_column":"col","transformation_logic":null,"confidence_score":0.9,"reasoning":"why"}}]"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "system", "content": "Return JSON only."}, {"role": "user", "content": prompt}],
                temperature=0.2
            )
            
            text = response.choices[0].message.content.strip()
            if 'json' in text and chr(96) in text:
                text = text.split(chr(96)*3)[1].replace('json','').strip()
            
            result = json.loads(text)
            if isinstance(result, dict):
                result = result.get('mappings', result.get('mapping', []))
            
            return [ETLMapping(m['source_node'], target_table, m['target_column'], 
                             m.get('transformation_logic'), float(m.get('confidence_score', 0.5)), m.get('reasoning', '')) 
                   for m in result]
        except Exception as e:
            logger.error(f"Failed: {e}")
            return []


class AIETLMapper(OpenAIMapper):
    pass
