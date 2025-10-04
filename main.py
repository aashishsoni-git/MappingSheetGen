# main.py - UPDATED with flexible loading
from extractors.xml_parser import XMLMetadataExtractor
from loaders.snowflake_loader import SnowflakeStageLoader
from mapper.openai_mapper import AIETLMapper
from utils.schema_manager import SchemaManager
from utils.product_detector import SmartProductDetector
from utils.document_loader import SmartReferenceDataMatcher  # NEW
from utils.logging_config import setup_logging
from utils.cost_estimator import estimate_mapping_cost
from utils.decorators import log_execution_time
import pandas as pd
import os
import logging
from dotenv import load_dotenv

load_dotenv()
setup_logging(log_level='INFO', log_file='logs/etl_mapper.log')
logger = logging.getLogger(__name__)


class ETLMappingPipeline:
    def __init__(self, openai_key, snowflake_config):
        self.xml_parser = XMLMetadataExtractor()
        self.sf_loader = SnowflakeStageLoader(**snowflake_config)
        self.ai_mapper = AIETLMapper(openai_key)
        self.schema_manager = SchemaManager(self.sf_loader)
        self.product_detector = SmartProductDetector()
        self.reference_matcher = SmartReferenceDataMatcher(openai_key)  # NEW
    
    @log_execution_time
    def run(self, xml_file_path, product_code=None, use_flexible_loader=True):
        """
        Execute full pipeline with smart product detection and flexible loading
        
        Args:
            xml_file_path: Path to XML file
            product_code: Optional product code. If None, will auto-detect
            use_flexible_loader: Use AI-powered flexible document loader
        """
        
        # Step 1: Detect product if not provided
        if not product_code:
            logger.info("🔍 Auto-detecting product type...")
            product_code, confidence, details = self.product_detector.detect_product(xml_file_path)
            
            if not product_code:
                raise ValueError("Could not detect product type. Please specify product_code manually.")
            
            logger.info(f"✅ Detected: {product_code} with {confidence:.1%} confidence")
        
        # Step 2: Extract XML metadata
        logger.info("📊 Extracting XML metadata...")
        xml_metadata = self.xml_parser.extract_schema(xml_file_path)
        logger.info(f"✅ Extracted {len(xml_metadata)} XML nodes")
        
        # Step 3: Load to Snowflake stage
        logger.info("☁️ Loading to Snowflake stage layer...")
        stage_table = f"ETL_MAPPER.STAGE_XML_{product_code}"
        self.sf_loader.load_xml_as_json(xml_file_path, stage_table)
        logger.info(f"✅ Loaded to {stage_table}")
        
        # Step 4: Get REAL Silver layer schema from Snowflake
        logger.info("🗄️ Fetching Silver layer schema from Snowflake...")
        silver_schema = self.schema_manager.get_silver_schema(
            database='INSURANCE',
            schema='SILVER'
        )
        logger.info(f"✅ Fetched {len(silver_schema)} columns from {len(silver_schema['table_name'].unique())} tables")
        
        # Step 5: Load reference data (FLEXIBLE or LEGACY)
        if use_flexible_loader:
            logger.info("📚 Finding and loading reference data with AI...")
            reference_data = self.reference_matcher.find_and_load_references(
                product_code=product_code,
                reference_directory='reference_data',
                max_files_per_category=3
            )
            
            # Step 6: Predict mappings with FLEXIBLE approach
            logger.info("🤖 Predicting mappings with AI (flexible mode)...")
            predictions = self.ai_mapper.predict_mappings_flexible(
                xml_metadata,
                silver_schema,
                reference_data
            )
        else:
            # LEGACY approach
            logger.info("📚 Loading reference data (legacy mode)...")
            hist_mappings = self.product_detector.load_relevant_mappings(
                product_code=product_code,
                mappings_directory='reference_data',
                max_files=3
            )
            logger.info(f"✅ Loaded {len(hist_mappings)} relevant historical mappings")
            
            # Load data dictionary
            data_dict_path = 'reference_data/data_dictionary.csv'
            if os.path.exists(data_dict_path):
                data_dict = pd.read_csv(data_dict_path)
            else:
                logger.warning("Data dictionary not found")
                data_dict = pd.DataFrame()
            
            # Step 6: Predict mappings (LEGACY)
            logger.info("🤖 Predicting mappings with AI (legacy mode)...")
            predictions = self.ai_mapper.predict_mappings(
                xml_metadata,
                silver_schema,
                hist_mappings,
                data_dict
            )
        
        # Step 7: Save results
        logger.info("💾 Saving results...")
        self._save_results(predictions, product_code)
        
        logger.info("✅ Pipeline completed successfully!")
        return predictions
    
    def _save_results(self, predictions, product_code):
        """Save predictions to CSV"""
        os.makedirs('output', exist_ok=True)
        
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'output/mappings_{product_code}_{timestamp}.csv'
        
        mappings_df = pd.DataFrame([m.dict() for m in predictions.mappings])
        mappings_df.to_csv(output_file, index=False)
        
        logger.info(f"📁 Saved {len(mappings_df)} mappings to {output_file}")
        
        # Print summary
        print("\n" + "="*80)
        print(f"MAPPING PREDICTIONS - {product_code}")
        print("="*80)
        print(f"Total mappings: {len(mappings_df)}")
        print(f"Average confidence: {mappings_df['confidence_score'].mean():.1%}")
        print(f"High confidence (≥80%): {len(mappings_df[mappings_df['confidence_score'] >= 0.8])}")
        print(f"Medium confidence (50-80%): {len(mappings_df[(mappings_df['confidence_score'] >= 0.5) & (mappings_df['confidence_score'] < 0.8)])}")
        print(f"Low confidence (<50%): {len(mappings_df[mappings_df['confidence_score'] < 0.5])}")
        print(f"\nOutput file: {output_file}")
        print("="*80 + "\n")


if __name__ == "__main__":
    config = {
        'openai_key': os.getenv('OPENAI_API_KEY'),
        'snowflake_config': {
            'account': os.getenv('SF_ACCOUNT'),
            'user': os.getenv('SF_USER'),
            'password': os.getenv('SF_PASSWORD'),
            'warehouse': os.getenv('SF_WAREHOUSE'),
            'database': os.getenv('SF_DATABASE'),
            'schema': 'ETL_MAPPER',
            'role': os.getenv('SF_ROLE')
        }
    }
    
    # Example: Use flexible loading (AI-powered)
    pipeline = ETLMappingPipeline(config['openai_key'], config['snowflake_config'])
    results = pipeline.run(
        'data/quote_personal_auto_001.xml',
        use_flexible_loader=True  # NEW: Enable AI-powered flexible loading
    )
