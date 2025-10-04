# extractors/xml_parser.py - COMPLETE FIXED VERSION
import xml.etree.ElementTree as ET
from lxml import etree
import pandas as pd


class XMLMetadataExtractor:
    def extract_schema(self, xml_path, xsd_path=None):
        """Extract XML structure, nodes, attributes, and sample values"""
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        metadata = []
        for elem in root.iter():
            node_path = self._get_full_path(elem, root)
            metadata.append({
                'node_path': node_path,
                'node_name': elem.tag,
                'attributes': list(elem.attrib.keys()),
                'sample_value': elem.text[:100] if elem.text and elem.text.strip() else None,
                'data_type': self._infer_type(elem.text),
                'parent_path': self._get_parent_path(node_path)
            })
        
        return pd.DataFrame(metadata)
    
    def _get_full_path(self, element, root):
        """Get full XPath of element"""
        # Build path from root to current element
        path_parts = []
        current = element
        
        # Walk up the tree
        while current is not None:
            path_parts.insert(0, current.tag)
            # Find parent
            parent = None
            for potential_parent in root.iter():
                if current in list(potential_parent):
                    parent = potential_parent
                    break
            current = parent
        
        return '/' + '/'.join(path_parts)
    
    def _get_parent_path(self, node_path):
        """Get parent path from full node path"""
        if '/' not in node_path or node_path.count('/') <= 1:
            return None
        
        # Split and remove last element
        parts = node_path.rsplit('/', 1)
        return parts[0] if parts[0] else None
    
    def _infer_type(self, value):
        """Infer data type from sample value"""
        if not value or not str(value).strip():
            return 'string'
        
        value_str = str(value).strip()
        
        # Try integer
        try:
            int(value_str)
            return 'integer'
        except:
            pass
        
        # Try float
        try:
            float(value_str)
            return 'decimal'
        except:
            pass
        
        # Try date (basic check)
        if len(value_str) == 10 and value_str.count('-') == 2:
            return 'date'
        
        # Try timestamp
        if 'T' in value_str and ':' in value_str:
            return 'timestamp'
        
        # Try boolean
        if value_str.lower() in ['true', 'false', 'yes', 'no', 'y', 'n']:
            return 'boolean'
        
        return 'string'
