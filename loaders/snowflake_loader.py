# loaders/snowflake_loader.py
import snowflake.connector
import json
import xml.etree.ElementTree as ET
import logging

logger = logging.getLogger(__name__)


class SnowflakeStageLoader:
    def __init__(self, account, user, password, warehouse, database, schema='STAGE', role='ACCOUNTADMIN'):
        """
        Initialize Snowflake connection
        
        Args:
            account: Snowflake account identifier (e.g., chswrge-ya35642)
            user: Snowflake username
            password: Snowflake password
            warehouse: Warehouse name
            database: Database name
            schema: Schema name (default: STAGE)
            role: Role name (default: ACCOUNTADMIN)
        """
        try:
            self.conn = snowflake.connector.connect(
                account=account,
                user=user,
                password=password,
                warehouse=warehouse,
                database=database,
                schema=schema,
                role=role
            )
            logger.info(f"✅ Connected to Snowflake: {database}.{schema} with role {role}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {str(e)}")
            raise
    
    def load_xml_as_json(self, xml_path, stage_table):
        """Load XML file as JSON variant to stage table"""
        # Parse XML and convert to JSON
        tree = ET.parse(xml_path)
        root = tree.getroot()
        json_data = self._xml_to_json(root)
        
        # Insert into Snowflake
        cursor = self.conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {stage_table} (
                xml_data VARIANT,
                load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        cursor.execute(
            f"INSERT INTO {stage_table} (xml_data) SELECT PARSE_JSON(%s)",
            (json.dumps(json_data),)
        )
        
        logger.info(f"✅ Loaded XML data to {stage_table}")
        cursor.close()
    
    def _xml_to_json(self, element):
        """Convert XML element to JSON-compatible dictionary"""
        result = {}
        
        # Add attributes
        if element.attrib:
            result['@attributes'] = element.attrib
        
        # Add text content
        if element.text and element.text.strip():
            result['#text'] = element.text.strip()
        
        # Add child elements
        children = {}
        for child in element:
            child_data = self._xml_to_json(child)
            
            if child.tag in children:
                # Handle multiple children with same tag
                if not isinstance(children[child.tag], list):
                    children[child.tag] = [children[child.tag]]
                children[child.tag].append(child_data)
            else:
                children[child.tag] = child_data
        
        if children:
            result.update(children)
        
        # If only text, return text directly
        if len(result) == 1 and '#text' in result:
            return result['#text']
        
        return result
    
    def test_connection(self):
        """Test Snowflake connection"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()")
            result = cursor.fetchone()
            logger.info(f"✅ Connection test successful!")
            logger.info(f"   Version: {result[0]}")
            logger.info(f"   Database: {result[1]}")
            logger.info(f"   Schema: {result[2]}")
            logger.info(f"   Role: {result[3]}")
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"❌ Connection test failed: {str(e)}")
            return False
    
    def close(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed")
