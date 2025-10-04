# mapper/similarity_engine.py
from openai import OpenAI
import numpy as np

class SemanticMatcher:
    def __init__(self, api_key):
        self.client = OpenAI(api_key=api_key)
    
    def calculate_similarity(self, source_fields, target_fields):
        """Calculate semantic similarity using embeddings"""
        
        # Get embeddings for all fields
        source_embeddings = self._get_embeddings(source_fields)
        target_embeddings = self._get_embeddings(target_fields)
        
        # Calculate cosine similarity matrix
        similarity_matrix = np.dot(source_embeddings, target_embeddings.T)
        
        return similarity_matrix
    
    def _get_embeddings(self, texts):
        """Get embeddings from OpenAI"""
        response = self.client.embeddings.create(
            model="text-embedding-3-small",
            input=texts
        )
        return np.array([item.embedding for item in response.data])
