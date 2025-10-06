# Add this to replace the _save_results method in main.py

def _save_results(self, predictions, product_code: str):
    """Save mapping results to output directory"""
    import pandas as pd
    from datetime import datetime
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"output/mappings_{product_code}_{timestamp}.csv"
    
    os.makedirs('output', exist_ok=True)
    
    # ✅ FIX: Convert predictions to DataFrame with proper columns
    mappings_data = []
    for mapping in predictions.mappings:
        mappings_data.append({
            'source_node': mapping.source_node,
            'target_table': mapping.target_table,
            'target_column': mapping.target_column,
            'transformation_logic': mapping.transformation_logic or '',
            'confidence_score': mapping.confidence_score,
            'reasoning': mapping.reasoning
        })
    
    mappings_df = pd.DataFrame(mappings_data)
    
    # Save to CSV
    mappings_df.to_csv(output_file, index=False)
    
    logger.info(f"✅ Saved {len(mappings_df)} mappings to {output_file}")
    
    # Print summary (with safe column access)
    if not mappings_df.empty and 'confidence_score' in mappings_df.columns:
        print(f"Average confidence: {mappings_df['confidence_score'].mean():.1%}")
    
    return output_file
