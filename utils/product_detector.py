# utils/product_detector.py
"""
Intelligent product detection using fuzzy matching and semantic similarity
"""
import pandas as pd
from typing import Dict, List, Tuple
import xml.etree.ElementTree as ET
from difflib import SequenceMatcher
import re
import logging
import os

logger = logging.getLogger(__name__)


class SmartProductDetector:
    """
    Intelligently detect product type and select relevant reference documents
    using fuzzy matching and content analysis
    """
    
    def __init__(self):
        self.product_definitions = self._load_product_definitions()
    
    def _load_product_definitions(self) -> pd.DataFrame:
        """
        Load product definitions from configuration file or database
        Should include: product_code, product_name, keywords, node_patterns, file_patterns
        """
        # This should ideally come from a config file or database
        # For now, using a flexible structure
        definitions = [
            {
                'product_code': 'PA001',
                'product_name': 'Personal Auto Insurance',
                'keywords': ['auto', 'vehicle', 'car', 'driver', 'vin', 'automobile', 'motorvehicle'],
                'node_patterns': ['Vehicle', 'Driver', 'VIN', 'Make', 'Model', 'VehicleInfo'],
                'attribute_patterns': ['vehicleidentificationnum', 'vehiclevin', 'driverage'],
                'weight': 1.0
            },
            {
                'product_code': 'HO003',
                'product_name': 'Homeowners Insurance',
                'keywords': ['home', 'homeowner', 'dwelling', 'property', 'residence', 'building', 'house'],
                'node_patterns': ['Dwelling', 'PropertyDetails', 'BuildingYear', 'ConstructionType', 'Occupancy'],
                'attribute_patterns': ['squarefootage', 'buildingYear', 'propertyvalue'],
                'weight': 1.0
            },
            {
                'product_code': 'CP001',
                'product_name': 'Commercial Property Insurance',
                'keywords': ['commercial', 'business', 'office', 'businessproperty', 'businesspersonalproperty'],
                'node_patterns': ['CommercialBuilding', 'BusinessProperty', 'BusinessName', 'OccupancyType'],
                'attribute_patterns': ['businessname', 'commercialbuilding'],
                'weight': 1.0
            },
            {
                'product_code': 'UMB001',
                'product_name': 'Personal Umbrella Insurance',
                'keywords': ['umbrella', 'excess', 'excessliability', 'personalliability', 'underlyingretention'],
                'node_patterns': ['Umbrella', 'ExcessLiability', 'UnderlyingRetention'],
                'attribute_patterns': ['underlyingretention', 'excessliability'],
                'weight': 1.0
            },
            {
                'product_code': 'WC001',
                'product_name': 'Workers Compensation',
                'keywords': ['workers', 'workerscomp', 'workerscompensation', 'employee', 'payroll'],
                'node_patterns': ['Employee', 'Payroll', 'WorkersComp', 'Occupation'],
                'attribute_patterns': ['employeecount', 'payrollamount'],
                'weight': 1.0
            }
        ]
        
        return pd.DataFrame(definitions)
    
    def detect_product(self, xml_file, min_score: float = 0.2) -> Tuple[str, float, Dict]:
        """
        Intelligently detect product from XML using multiple signals
        
        Args:
            xml_file: Path to XML file or file-like object
            min_score: Minimum confidence score to return detection
            
        Returns:
            Tuple of (product_code, confidence_score, detailed_scores)
        """
        try:
            # Parse XML
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            # Extract all relevant text from XML
            xml_text = ET.tostring(root, encoding='unicode').lower()
            all_nodes = [elem.tag.lower() for elem in root.iter()]
            all_attributes = []
            for elem in root.iter():
                all_attributes.extend([attr.lower() for attr in elem.attrib.keys()])
            
            # Score each product
            product_scores = {}
            detailed_scores = {}
            
            for _, product in self.product_definitions.iterrows():
                score = 0
                details = {
                    'keyword_matches': 0,
                    'node_matches': 0,
                    'attribute_matches': 0,
                    'fuzzy_score': 0
                }
                
                # 1. Exact keyword matching (weight: 1.0)
                for keyword in product['keywords']:
                    if keyword in xml_text:
                        score += 1.0
                        details['keyword_matches'] += 1
                
                # 2. Node pattern matching with fuzzy matching (weight: 2.0)
                for pattern in product['node_patterns']:
                    pattern_lower = pattern.lower()
                    # Exact match
                    if pattern_lower in all_nodes:
                        score += 2.0
                        details['node_matches'] += 1
                    else:
                        # Fuzzy match
                        for node in all_nodes:
                            similarity = self._fuzzy_match(pattern_lower, node)
                            if similarity > 0.8:  # 80% similarity threshold
                                score += 1.5
                                details['node_matches'] += 1
                                details['fuzzy_score'] += similarity
                                break
                
                # 3. Attribute pattern matching (weight: 1.5)
                for pattern in product['attribute_patterns']:
                    pattern_lower = pattern.lower()
                    if pattern_lower in all_attributes:
                        score += 1.5
                        details['attribute_matches'] += 1
                    else:
                        # Fuzzy match
                        for attr in all_attributes:
                            similarity = self._fuzzy_match(pattern_lower, attr)
                            if similarity > 0.8:
                                score += 1.0
                                details['attribute_matches'] += 1
                                break
                
                # 4. Product name in XML (weight: 3.0)
                if product['product_code'].lower() in xml_text:
                    score += 3.0
                
                # Normalize score by number of possible matches
                max_possible_score = (
                    len(product['keywords']) * 1.0 +
                    len(product['node_patterns']) * 2.0 +
                    len(product['attribute_patterns']) * 1.5 +
                    3.0
                )
                
                normalized_score = score / max_possible_score if max_possible_score > 0 else 0
                
                product_scores[product['product_code']] = normalized_score
                detailed_scores[product['product_code']] = {
                    'raw_score': score,
                    'normalized_score': normalized_score,
                    'details': details,
                    'product_name': product['product_name']
                }
            
            # Get best match
            if not product_scores or max(product_scores.values()) < min_score:
                logger.warning("No confident product match found")
                return None, 0.0, detailed_scores
            
            best_product = max(product_scores, key=product_scores.get)
            confidence = product_scores[best_product]
            
            logger.info(f"✅ Detected product: {best_product} with {confidence:.1%} confidence")
            
            return best_product, confidence, detailed_scores
            
        except Exception as e:
            logger.error(f"Error detecting product: {str(e)}")
            return None, 0.0, {}
    
    def _fuzzy_match(self, str1: str, str2: str) -> float:
        """Calculate fuzzy string similarity (0-1)"""
        return SequenceMatcher(None, str1, str2).ratio()
    
    def find_relevant_mappings(self, product_code: str, 
                               mappings_directory: str = 'reference_data') -> List[str]:
        """
        Find all relevant mapping files for a product using fuzzy filename matching
        
        Args:
            product_code: Detected product code (e.g., 'PA001')
            mappings_directory: Directory containing mapping files
            
        Returns:
            List of relevant mapping file paths, sorted by relevance
        """
        if not os.path.exists(mappings_directory):
            logger.warning(f"Mappings directory not found: {mappings_directory}")
            return []
        
        # Get product info
        product_info = self.product_definitions[
            self.product_definitions['product_code'] == product_code
        ]
        
        if product_info.empty:
            logger.warning(f"Unknown product code: {product_code}")
            return []
        
        product_keywords = product_info.iloc[0]['keywords']
        product_name = product_info.iloc[0]['product_name']
        
        # Find all CSV files in directory
        all_files = [f for f in os.listdir(mappings_directory) if f.endswith('.csv')]
        
        # Score each file by relevance
        file_scores = {}
        
        for filename in all_files:
            score = 0
            filename_lower = filename.lower()
            
            # Exact product code match (highest priority)
            if product_code.lower() in filename_lower:
                score += 10.0
            
            # Product keywords in filename
            for keyword in product_keywords:
                if keyword in filename_lower:
                    score += 2.0
            
            # Fuzzy match on product name parts
            product_name_parts = re.findall(r'\w+', product_name.lower())
            for part in product_name_parts:
                if len(part) > 3:  # Skip short words like 'and', 'the'
                    # Exact match
                    if part in filename_lower:
                        score += 1.5
                    else:
                        # Fuzzy match
                        for word in re.findall(r'\w+', filename_lower):
                            if self._fuzzy_match(part, word) > 0.85:
                                score += 1.0
                                break
            
            # Generic mapping file (lower priority)
            if 'mapping' in filename_lower and 'old' in filename_lower:
                score += 0.5
            
            if score > 0:
                file_scores[filename] = score
        
        # Sort by score (descending)
        sorted_files = sorted(file_scores.items(), key=lambda x: x[1], reverse=True)
        
        # Return file paths
        relevant_files = [
            os.path.join(mappings_directory, filename) 
            for filename, score in sorted_files 
            if score >= 1.0  # Minimum relevance threshold
        ]
        
        logger.info(f"Found {len(relevant_files)} relevant mapping files for {product_code}")
        for filepath in relevant_files:
            logger.info(f"  - {os.path.basename(filepath)} (score: {file_scores[os.path.basename(filepath)]:.1f})")
        
        return relevant_files
    
    def load_relevant_mappings(self, product_code: str, 
                               mappings_directory: str = 'reference_data',
                               max_files: int = 3) -> pd.DataFrame:
        """
        Load and combine relevant mapping files for a product
        
        Args:
            product_code: Product code
            mappings_directory: Directory with mapping files
            max_files: Maximum number of files to load
            
        Returns:
            Combined DataFrame of relevant mappings
        """
        relevant_files = self.find_relevant_mappings(product_code, mappings_directory)
        
        if not relevant_files:
            logger.warning(f"No relevant mapping files found for {product_code}")
            # Try loading generic mappings file
            generic_file = os.path.join(mappings_directory, 'old_mappings.csv')
            if os.path.exists(generic_file):
                logger.info("Loading generic mappings file")
                return pd.read_csv(generic_file)
            return pd.DataFrame()
        
        # Load top N relevant files
        all_mappings = []
        for filepath in relevant_files[:max_files]:
            try:
                df = pd.read_csv(filepath)
                df['source_file'] = os.path.basename(filepath)
                all_mappings.append(df)
                logger.info(f"Loaded {len(df)} mappings from {os.path.basename(filepath)}")
            except Exception as e:
                logger.error(f"Error loading {filepath}: {str(e)}")
        
        if not all_mappings:
            return pd.DataFrame()
        
        # Combine all mappings
        combined_df = pd.concat(all_mappings, ignore_index=True)
        
        # Remove duplicates (prefer mappings from more relevant files)
        combined_df = combined_df.drop_duplicates(
            subset=['source_node', 'target_column'],
            keep='first'
        )
        
        logger.info(f"✅ Loaded {len(combined_df)} total mappings from {len(all_mappings)} files")
        
        return combined_df
