# utils/document_loader.py
"""
AI-powered flexible document loader - handles ANY format
"""
from openai import OpenAI
import pandas as pd
import os
from typing import Dict, Any, List
import logging
import json

logger = logging.getLogger(__name__)


class FlexibleDocumentLoader:
    """
    Load and understand documents of any format using AI
    No hardcoded schemas - AI infers structure
    """
    
    def __init__(self, openai_key: str):
        self.client = OpenAI(api_key=openai_key)
        self.model = "gpt-4o-mini"  # Cheaper model for document understanding
    
    def load_any_file(self, filepath: str) -> Dict[str, Any]:
        """
        Load any file type and let AI understand its structure
        Supports: CSV, XLSX, XLS, TXT, JSON
        """
        ext = os.path.splitext(filepath)[1].lower()
        
        logger.info(f"Loading file: {filepath} (type: {ext})")
        
        try:
            if ext in ['.csv', '.txt']:
                return self._load_delimited_file(filepath)
            elif ext in ['.xlsx', '.xls']:
                return self._load_excel_file(filepath)
            elif ext == '.json':
                return self._load_json_file(filepath)
            else:
                raise ValueError(f"Unsupported file type: {ext}")
        except Exception as e:
            logger.error(f"Error loading {filepath}: {e}")
            return {'error': str(e), 'filepath': filepath}
    
    def _load_delimited_file(self, filepath: str) -> Dict[str, Any]:
        """
        Load CSV/TXT with AI-detected delimiter and structure
        """
        # Read first 20 lines to understand structure
        with open(filepath, 'r', encoding='utf-8') as f:
            sample_lines = [f.readline() for _ in range(20)]
        
        sample_text = ''.join(sample_lines)
        
        # Ask AI to detect delimiter and structure
        prompt = f"""Analyze this file sample and tell me:
1. What is the delimiter? (comma, semicolon, tab, pipe, etc.)
2. Does it have a header row?
3. How many columns are there?
4. What type of data is this? (mappings, dictionary, reference data, etc.)

File sample:
{sample_text}

Return JSON with: {{"delimiter": "...", "has_header": true/false, "num_columns": N, "data_type": "...", "inferred_column_names": [...]}}
"""
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        
        structure = eval(response.choices[0].message.content)
        logger.info(f"AI detected structure: {structure}")
        
        # Load with detected parameters
        delimiter_map = {
            'comma': ',',
            'semicolon': ';',
            'tab': '\t',
            'pipe': '|',
            'space': ' '
        }
        
        delimiter = delimiter_map.get(structure['delimiter'].lower(), ',')
        
        try:
            df = pd.read_csv(
                filepath,
                delimiter=delimiter,
                header=0 if structure.get('has_header') else None,
                encoding='utf-8',
                on_bad_lines='skip'  # Handle malformed rows
            )
        except:
            # Fallback: try different encodings
            df = pd.read_csv(
                filepath,
                delimiter=delimiter,
                header=0 if structure.get('has_header') else None,
                encoding='latin-1',
                on_bad_lines='skip'
            )
        
        return {
            'dataframe': df,
            'structure': structure,
            'filepath': filepath,
            'row_count': len(df),
            'column_count': len(df.columns)
        }
    
    def _load_excel_file(self, filepath: str) -> Dict[str, Any]:
        """
        Load Excel file - AI understands structure regardless of format
        """
        # Read all sheets
        xl_file = pd.ExcelFile(filepath)
        sheet_names = xl_file.sheet_names
        
        logger.info(f"Found {len(sheet_names)} sheets: {sheet_names}")
        
        # Load first sheet by default
        df = pd.read_excel(filepath, sheet_name=sheet_names[0])
        
        # Get sample to understand structure
        sample = df.head(10).to_string()
        
        # Ask AI what this data represents
        prompt = f"""Analyze this Excel data and tell me what it contains:

Sheet: {sheet_names[0]}
Sample data: {sample}

Return JSON with: {{"data_type": "...", "description": "...", "key_columns": [...], "purpose": "..."}}
Examples of data_type: "historical_mappings", "data_dictionary", "reference_data", "schema_definition"
"""
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        
        understanding = eval(response.choices[0].message.content)
        logger.info(f"AI understands this as: {understanding['data_type']}")
        
        return {
            'dataframe': df,
            'understanding': understanding,
            'filepath': filepath,
            'sheets': sheet_names,
            'row_count': len(df),
            'column_count': len(df.columns)
        }
    
    def _load_json_file(self, filepath: str) -> Dict[str, Any]:
        """Load JSON file"""
        df = pd.read_json(filepath)
        return {
            'dataframe': df,
            'filepath': filepath,
            'row_count': len(df)
        }


class SmartReferenceDataMatcher:
    """
    Intelligently match and categorize reference files
    No hardcoded names - uses fuzzy matching and AI
    """
    
    def __init__(self, openai_key: str):
        self.client = OpenAI(api_key=openai_key)
        self.model = "gpt-4o-mini"
        self.loader = FlexibleDocumentLoader(openai_key)
    
# utils/document_loader.py - Fix the method signature around line 150
# Find this method and update it:

  # Replace the find_and_load_references method in utils/document_loader.py
    def find_and_load_references(self, 
                                product_code: str,
                                reference_directory: str,
                                max_files_per_category: int = 3) -> Dict[str, Any]:
        """
        Find and load ALL relevant reference files for a product
        AI determines what each file is for
        
        Args:
            product_code: Product code (e.g., PA001)
            reference_directory: Directory to search for files
            max_files_per_category: Maximum files to load per category (default: 3)
        
        Returns:
            Dict with categorized reference data
        """
        if not os.path.exists(reference_directory):
            logger.warning(f"Reference directory not found: {reference_directory}")
            return self._empty_references()
        
        # Get all files in directory
        all_files = []
        for root, dirs, files in os.walk(reference_directory):
            for file in files:
                if file.endswith(('.csv', '.xlsx', '.xls', '.json', '.txt')):
                    all_files.append(os.path.join(root, file))
        
        logger.info(f"🔍 Found {len(all_files)} potential reference files")
        
        if not all_files:
            logger.warning("No reference files found")
            return self._empty_references()
        
        # Ask AI to categorize each file
        file_classifications = {}
        
        for filepath in all_files:
            filename = os.path.basename(filepath)
            
            # Quick classification based on filename and product
            classification_prompt = f"""Given this filename and product code, classify what type of reference data this might be:

    Filename: {filename}
    Product Code: {product_code}

    Classify as ONE of:
    - "historical_mapping": Past ETL mappings for this product
    - "data_dictionary": Column definitions and business rules
    - "schema_definition": Database schema information
    - "reference_data": Lookup tables or reference values
    - "not_relevant": Not relevant for this product

    Also provide a relevance_score (0-100) for how relevant this is to product {product_code}.
    If filename contains product code or similar keywords, increase relevance.

    Return ONLY valid JSON: {{"classification": "historical_mapping/data_dictionary/schema_definition/reference_data/not_relevant", "relevance_score": N, "reasoning": "brief reason"}}
    """
            
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": classification_prompt}],
                    response_format={"type": "json_object"},
                    temperature=0.3
                )
                
                classification = json.loads(response.choices[0].message.content)
                
                if classification['classification'] != 'not_relevant' and classification['relevance_score'] > 20:
                    logger.info(f"📄 {filename}: {classification['classification']} (relevance: {classification['relevance_score']}%)")
                    file_classifications[filepath] = classification
            except Exception as e:
                logger.warning(f"Failed to classify {filename}: {e}")
                continue
        
        # Load the relevant files
        loaded_data = {
            'historical_mappings': [],
            'data_dictionaries': [],
            'schema_definitions': [],
            'reference_data': []
        }
        
        # Sort by relevance and load top files per category
        for filepath, classification in sorted(
            file_classifications.items(),
            key=lambda x: x[1]['relevance_score'],
            reverse=True
        ):
            category_map = {
                'historical_mapping': 'historical_mappings',
                'data_dictionary': 'data_dictionaries',
                'schema_definition': 'schema_definitions',
                'reference_data': 'reference_data'
            }
            
            category = category_map.get(classification['classification'])
            
            if category and len(loaded_data[category]) < max_files_per_category:
                loaded = self.loader.load_any_file(filepath)
                
                if 'error' not in loaded:
                    loaded_data[category].append({
                        'data': loaded,
                        'classification': classification,
                        'filepath': filepath,
                        'filename': os.path.basename(filepath)
                    })
        
        # Log summary
        logger.info(f"\n{'='*60}")
        logger.info(f"✅ LOADED REFERENCE DATA SUMMARY:")
        logger.info(f"{'='*60}")
        logger.info(f"  📚 Historical mappings: {len(loaded_data['historical_mappings'])} files")
        for item in loaded_data['historical_mappings']:
            logger.info(f"     - {item['filename']} ({item['data']['row_count']} rows)")
        
        logger.info(f"  📖 Data dictionaries: {len(loaded_data['data_dictionaries'])} files")
        for item in loaded_data['data_dictionaries']:
            logger.info(f"     - {item['filename']} ({item['data']['row_count']} rows)")
        
        logger.info(f"  🗄️ Schema definitions: {len(loaded_data['schema_definitions'])} files")
        for item in loaded_data['schema_definitions']:
            logger.info(f"     - {item['filename']} ({item['data']['row_count']} rows)")
        
        logger.info(f"  📊 Reference data: {len(loaded_data['reference_data'])} files")
        logger.info(f"{'='*60}\n")
        
        return loaded_data

