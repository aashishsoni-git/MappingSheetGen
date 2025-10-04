# utils/schema_manager.py
"""
Dynamic schema management - fetches real-time schema from Snowflake
"""
import pandas as pd
import logging
from typing import Optional, List

logger = logging.getLogger(__name__)


class SchemaManager:
    """Dynamically fetch and manage database schemas"""
    
    def __init__(self, snowflake_loader):
        self.sf_loader = snowflake_loader
        self.schema_cache = {}
    
    def get_silver_schema(self, database: str = 'INSURANCE', schema: str = 'SILVER', 
                         tables: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Fetch actual Silver layer schema from Snowflake at runtime
        
        Args:
            database: Database name
            schema: Schema name
            tables: Optional list of specific tables to fetch. If None, fetches all.
        
        Returns:
            DataFrame with columns: table_name, column_name, data_type, description, 
                                   is_nullable, column_default, ordinal_position
        """
        cache_key = f"{database}.{schema}"
        
        # Check cache first (optional - can remove for always-fresh data)
        if cache_key in self.schema_cache:
            logger.info(f"Using cached schema for {cache_key}")
            cached_df = self.schema_cache[cache_key]
            if tables:
                return cached_df[cached_df['table_name'].str.upper().isin([t.upper() for t in tables])]
            return cached_df
        
        try:
            cursor = self.sf_loader.conn.cursor()
            
            # Build query to fetch schema metadata
            table_filter = ""
            if tables:
                table_list = "','".join([t.upper() for t in tables])
                table_filter = f"AND TABLE_NAME IN ('{table_list}')"
            
            query = f"""
                SELECT 
                    CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) as table_name,
                    COLUMN_NAME as column_name,
                    DATA_TYPE as data_type,
                    COALESCE(COMMENT, '') as description,
                    IS_NULLABLE as is_nullable,
                    COLUMN_DEFAULT as column_default,
                    ORDINAL_POSITION as ordinal_position,
                    CHARACTER_MAXIMUM_LENGTH as max_length,
                    NUMERIC_PRECISION as numeric_precision,
                    NUMERIC_SCALE as numeric_scale
                FROM {database}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema}'
                {table_filter}
                ORDER BY TABLE_NAME, ORDINAL_POSITION
            """
            
            logger.info(f"Fetching schema from {database}.{schema}")
            cursor.execute(query)
            
            result = cursor.fetchall()
            cursor.close()
            
            if not result:
                logger.warning(f"No schema found for {database}.{schema}")
                return self._get_fallback_schema()
            
            # Convert to DataFrame
            schema_df = pd.DataFrame(result, columns=[
                'table_name', 'column_name', 'data_type', 'description',
                'is_nullable', 'column_default', 'ordinal_position',
                'max_length', 'numeric_precision', 'numeric_scale'
            ])
            
            # Format data_type with precision/scale
            schema_df['formatted_data_type'] = schema_df.apply(
                lambda row: self._format_data_type(row), axis=1
            )
            
            logger.info(f"✅ Fetched {len(schema_df)} columns from {len(schema_df['table_name'].unique())} tables")
            
            # Cache the result
            self.schema_cache[cache_key] = schema_df
            
            return schema_df
            
        except Exception as e:
            logger.error(f"Error fetching schema from Snowflake: {str(e)}")
            logger.warning("Falling back to sample schema")
            return self._get_fallback_schema()
    
    def _format_data_type(self, row):
        """Format data type with precision/scale/length"""
        dtype = row['data_type']
        
        if dtype in ['VARCHAR', 'CHAR', 'TEXT'] and pd.notna(row['max_length']):
            return f"{dtype}({int(row['max_length'])})"
        elif dtype in ['NUMBER', 'DECIMAL', 'NUMERIC'] and pd.notna(row['numeric_precision']):
            if pd.notna(row['numeric_scale']) and row['numeric_scale'] > 0:
                return f"{dtype}({int(row['numeric_precision'])},{int(row['numeric_scale'])})"
            return f"{dtype}({int(row['numeric_precision'])})"
        else:
            return dtype
    
    def _get_fallback_schema(self):
        """Fallback schema if Snowflake query fails"""
        logger.warning("Using fallback schema - this should only happen in dev/testing")
        return pd.DataFrame({
            'table_name': ['SILVER.POLICY', 'SILVER.COVERAGE'],
            'column_name': ['policy_id', 'coverage_id'],
            'data_type': ['VARCHAR', 'VARCHAR'],
            'formatted_data_type': ['VARCHAR(50)', 'VARCHAR(50)'],
            'description': ['Policy identifier', 'Coverage identifier'],
            'is_nullable': ['NO', 'NO'],
            'ordinal_position': [1, 1]
        })
    
    def get_table_list(self, database: str = 'INSURANCE', schema: str = 'SILVER') -> List[str]:
        """Get list of all tables in schema"""
        try:
            cursor = self.sf_loader.conn.cursor()
            cursor.execute(f"""
                SELECT TABLE_NAME 
                FROM {database}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema}'
                AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            
            result = cursor.fetchall()
            cursor.close()
            
            tables = [row[0] for row in result]
            logger.info(f"Found {len(tables)} tables in {database}.{schema}")
            return tables
            
        except Exception as e:
            logger.error(f"Error fetching table list: {str(e)}")
            return ['POLICY', 'QUOTE', 'RISK', 'COVERAGE', 'PAYMENT']
    
    def refresh_schema(self):
        """Clear cache and force refresh on next fetch"""
        self.schema_cache.clear()
        logger.info("Schema cache cleared")
