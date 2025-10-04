# test_pipeline.py
"""
Comprehensive test script for the ETL Mapping Generator
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import logging

# Setup
load_dotenv()
from utils.logging_config import setup_logging
setup_logging(log_level='INFO', log_file='logs/test_pipeline.log')
logger = logging.getLogger(__name__)

def test_environment():
    """Test 1: Environment Setup"""
    print("\n" + "="*80)
    print("TEST 1: ENVIRONMENT SETUP")
    print("="*80)
    
    checks = {
        'OPENAI_API_KEY': os.getenv('OPENAI_API_KEY'),
        'SF_ACCOUNT': os.getenv('SF_ACCOUNT'),
        'SF_USER': os.getenv('SF_USER'),
        'SF_PASSWORD': os.getenv('SF_PASSWORD'),
        'SF_DATABASE': os.getenv('SF_DATABASE'),
    }
    
    all_passed = True
    for key, value in checks.items():
        status = "✅ PASS" if value else "❌ FAIL"
        print(f"{key}: {status}")
        if not value:
            all_passed = False
    
    return all_passed

def test_snowflake_connection():
    """Test 2: Snowflake Connection"""
    print("\n" + "="*80)
    print("TEST 2: SNOWFLAKE CONNECTION")
    print("="*80)
    
    try:
        from loaders.snowflake_loader import SnowflakeStageLoader
        
        config = {
            'account': os.getenv('SF_ACCOUNT'),
            'user': os.getenv('SF_USER'),
            'password': os.getenv('SF_PASSWORD'),
            'warehouse': os.getenv('SF_WAREHOUSE'),
            'database': os.getenv('SF_DATABASE'),
            'schema': 'ETL_MAPPER',
            'role': os.getenv('SF_ROLE')
        }
        
        loader = SnowflakeStageLoader(**config)
        success = loader.test_connection()
        loader.close()
        
        if success:
            print("✅ PASS: Snowflake connection successful")
            return True
        else:
            print("❌ FAIL: Snowflake connection failed")
            return False
            
    except Exception as e:
        print(f"❌ FAIL: {str(e)}")
        return False

def test_silver_schema_fetch():
    """Test 3: Dynamic Schema Fetching"""
    print("\n" + "="*80)
    print("TEST 3: DYNAMIC SILVER SCHEMA FETCHING")
    print("="*80)
    
    try:
        from loaders.snowflake_loader import SnowflakeStageLoader
        from utils.schema_manager import SchemaManager
        
        config = {
            'account': os.getenv('SF_ACCOUNT'),
            'user': os.getenv('SF_USER'),
            'password': os.getenv('SF_PASSWORD'),
            'warehouse': os.getenv('SF_WAREHOUSE'),
            'database': os.getenv('SF_DATABASE'),
            'schema': 'SILVER',
            'role': os.getenv('SF_ROLE')
        }
        
        loader = SnowflakeStageLoader(**config)
        schema_mgr = SchemaManager(loader)
        
        # Fetch schema
        silver_schema = schema_mgr.get_silver_schema(
            database='INSURANCE',
            schema='SILVER'
        )
        
        print(f"✅ Fetched schema with {len(silver_schema)} columns")
        print(f"✅ Tables found: {silver_schema['table_name'].unique().tolist()}")
        print("\nSample columns:")
        print(silver_schema[['table_name', 'column_name', 'data_type']].head(10))
        
        loader.close()
        return True
        
    except Exception as e:
        print(f"❌ FAIL: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_product_detection():
    """Test 4: Smart Product Detection"""
    print("\n" + "="*80)
    print("TEST 4: SMART PRODUCT DETECTION")
    print("="*80)
    
    try:
        from utils.product_detector import SmartProductDetector
        
        detector = SmartProductDetector()
        
        # Test files
        test_files = [
            ('data/quote_personal_auto_001.xml', 'PA001'),
            ('data/policy_homeowners_001.xml', 'HO003'),
            ('data/policy_commercial_property_001.xml', 'CP001'),
            ('data/policy_umbrella_001.xml', 'UMB001'),
        ]
        
        all_passed = True
        for filepath, expected_product in test_files:
            if not os.path.exists(filepath):
                print(f"⚠️ SKIP: {filepath} not found")
                continue
            
            print(f"\nTesting: {filepath}")
            detected, confidence, details = detector.detect_product(filepath)
            
            if detected == expected_product:
                print(f"  ✅ PASS: Detected {detected} (expected {expected_product}) - {confidence:.1%} confidence")
            else:
                print(f"  ❌ FAIL: Detected {detected} (expected {expected_product}) - {confidence:.1%} confidence")
                all_passed = False
            
            # Show top 3 scores
            sorted_products = sorted(details.items(), key=lambda x: x[1]['normalized_score'], reverse=True)[:3]
            print(f"  Top detections:")
            for prod, score_info in sorted_products:
                print(f"    - {prod}: {score_info['normalized_score']:.1%} (keywords: {score_info['details']['keyword_matches']}, nodes: {score_info['details']['node_matches']})")
        
        return all_passed
        
    except Exception as e:
        print(f"❌ FAIL: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_smart_mapping_selection():
    """Test 5: Smart Reference Mapping Selection"""
    print("\n" + "="*80)
    print("TEST 5: SMART REFERENCE MAPPING SELECTION")
    print("="*80)
    
    try:
        from utils.product_detector import SmartProductDetector
        
        detector = SmartProductDetector()
        
        test_products = ['PA001', 'HO003', 'CP001']
        
        all_passed = True
        for product in test_products:
            print(f"\nFinding mappings for: {product}")
            
            # Find relevant files
            relevant_files = detector.find_relevant_mappings(
                product_code=product,
                mappings_directory='reference_data'
            )
            
            if relevant_files:
                print(f"  ✅ Found {len(relevant_files)} relevant files:")
                for f in relevant_files:
                    print(f"    - {os.path.basename(f)}")
                
                # Load mappings
                mappings = detector.load_relevant_mappings(product)
                print(f"  ✅ Loaded {len(mappings)} total mappings")
            else:
                print(f"  ⚠️ WARNING: No relevant files found for {product}")
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        print(f"❌ FAIL: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_xml_parsing():
    """Test 6: XML Parsing and Metadata Extraction"""
    print("\n" + "="*80)
    print("TEST 6: XML PARSING AND METADATA EXTRACTION")
    print("="*80)
    
    try:
        from extractors.xml_parser import XMLMetadataExtractor
        
        parser = XMLMetadataExtractor()
        test_file = 'data/policy_homeowners_001.xml'
        
        if not os.path.exists(test_file):
            print(f"⚠️ SKIP: {test_file} not found")
            return False
        
        print(f"Parsing: {test_file}")
        metadata = parser.extract_schema(test_file)
        
        print(f"✅ Extracted {len(metadata)} nodes")
        print("\nSample metadata:")
        print(metadata[['node_name', 'data_type', 'sample_value']].head(10))
        
        return True
        
    except Exception as e:
        print(f"❌ FAIL: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_full_pipeline():
    """Test 7: Full End-to-End Pipeline"""
    print("\n" + "="*80)
    print("TEST 7: FULL END-TO-END PIPELINE")
    print("="*80)
    
    try:
        from main import ETLMappingPipeline
        
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
        
        pipeline = ETLMappingPipeline(config['openai_key'], config['snowflake_config'])
        
        # Test with one file
        test_file = 'data/quote_personal_auto_001.xml'
        
        if not os.path.exists(test_file):
            print(f"⚠️ SKIP: {test_file} not found")
            return False
        
        print(f"\n🚀 Running pipeline on: {test_file}")
        print("This will call OpenAI API and may take 30-60 seconds...\n")
        
        # Run pipeline (will auto-detect product)
        results = pipeline.run(test_file)
        
        print("\n✅ PASS: Pipeline completed successfully!")
        print(f"Generated {len(results.mappings)} mappings")
        
        # Show sample results
        print("\nSample mappings:")
        for i, mapping in enumerate(results.mappings[:5]):
            print(f"\n{i+1}. {mapping.source_node} → {mapping.target_column}")
            print(f"   Confidence: {mapping.confidence_score:.1%}")
            print(f"   Transform: {mapping.transformation_logic or 'None'}")
        
        return True
        
    except Exception as e:
        print(f"❌ FAIL: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests"""
    print("\n" + "🎯"*40)
    print("ETL MAPPING GENERATOR - COMPREHENSIVE TEST SUITE")
    print("🎯"*40)
    
    tests = [
        ("Environment Setup", test_environment),
        ("Snowflake Connection", test_snowflake_connection),
        ("Silver Schema Fetching", test_silver_schema_fetch),
        ("Product Detection", test_product_detection),
        ("Smart Mapping Selection", test_smart_mapping_selection),
        ("XML Parsing", test_xml_parsing),
        ("Full Pipeline", test_full_pipeline),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"\n❌ TEST CRASHED: {test_name}")
            print(f"Error: {str(e)}")
            results[test_name] = False
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    
    passed = sum(1 for r in results.values() if r)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    print("\n" + "="*80)
    print(f"TOTAL: {passed}/{total} tests passed ({passed/total*100:.0f}%)")
    print("="*80 + "\n")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
