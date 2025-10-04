"""
Test the flexible AI-powered document loading system
"""
import os
import sys
from dotenv import load_dotenv
import logging

load_dotenv()
from utils.logging_config import setup_logging
setup_logging(log_level='INFO', log_file='logs/test_flexible.log')
logger = logging.getLogger(__name__)

def test_flexible_system():
    """Test the complete flexible system"""
    print("\n" + "="*80)
    print("TESTING FLEXIBLE AI-POWERED ETL MAPPING SYSTEM")
    print("="*80 + "\n")
    
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
    
    # Test with the new comprehensive XML
    test_file = 'data/quote_personal_auto_001.xml'
    
    if not os.path.exists(test_file):
        print(f"❌ Test file not found: {test_file}")
        return False
    
    print(f"🚀 Testing with: {test_file}")
    print("   This XML has 150+ nodes with deep nesting")
    print("   Using AI-powered flexible document loading")
    print("   Reference data can be Excel, CSV, any format\n")
    
    try:
        # Run with FLEXIBLE mode (AI-powered)
        results = pipeline.run(
            test_file,
            use_flexible_loader=True  # Enable AI-powered flexible loading
        )
        
        print("\n" + "="*80)
        print("✅ TEST PASSED!")
        print("="*80)
        print(f"Generated {len(results.mappings)} mappings")
        print(f"Average confidence: {sum(m.confidence_score for m in results.mappings) / len(results.mappings):.1%}")
        
        # Show sample high-confidence mappings
        high_conf = [m for m in results.mappings if m.confidence_score >= 0.8]
        print(f"\nHigh confidence mappings (≥80%): {len(high_conf)}")
        print("\nSample high-confidence mappings:")
        for m in high_conf[:5]:
            print(f"  • {m.source_node} → {m.target_table}.{m.target_column} ({m.confidence_score:.1%})")
        
        return True
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_flexible_system()
    sys.exit(0 if success else 1)
