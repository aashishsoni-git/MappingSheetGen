# utils/cost_estimator.py
"""
Cost estimation utilities for OpenAI API usage
"""
from typing import Dict, Optional
import tiktoken

class CostEstimator:
    """Estimate and track OpenAI API costs"""
    
    # Pricing as of Oct 2025 (per 1K tokens)
    PRICING = {
        'gpt-4o-2024-08-06': {
            'input': 0.0025,
            'output': 0.01
        },
        'text-embedding-3-small': {
            'input': 0.00002,
            'output': 0.0
        }
    }
    
    def __init__(self, model: str = 'gpt-4o-2024-08-06'):
        self.model = model
        self.encoding = tiktoken.encoding_for_model(model)
        self.total_cost = 0.0
        self.call_history = []
    
    def estimate_cost(self, 
                     xml_metadata_size: int, 
                     schema_size: int,
                     historical_mappings_size: int) -> Dict[str, float]:
        """
        Estimate cost for a single mapping prediction call
        
        Args:
            xml_metadata_size: Number of XML nodes
            schema_size: Number of target columns
            historical_mappings_size: Number of historical mapping rows
            
        Returns:
            Dictionary with cost breakdown
        """
        # Rough token estimation
        input_tokens = (
            xml_metadata_size * 50 +      # ~50 tokens per XML node
            schema_size * 40 +             # ~40 tokens per column
            historical_mappings_size * 60 + # ~60 tokens per mapping
            500                            # System prompt + formatting
        )
        
        output_tokens = schema_size * 80   # ~80 tokens per predicted mapping
        
        pricing = self.PRICING[self.model]
        input_cost = (input_tokens / 1000) * pricing['input']
        output_cost = (output_tokens / 1000) * pricing['output']
        total_cost = input_cost + output_cost
        
        return {
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'input_cost': input_cost,
            'output_cost': output_cost,
            'total_cost': total_cost,
            'model': self.model
        }
    
    def count_tokens(self, text: str) -> int:
        """Count actual tokens in text"""
        return len(self.encoding.encode(text))
    
    def track_actual_usage(self, response) -> Dict[str, float]:
        """
        Track actual API usage from OpenAI response
        
        Args:
            response: OpenAI API response object
        """
        usage = response.usage
        pricing = self.PRICING[self.model]
        
        input_cost = (usage.prompt_tokens / 1000) * pricing['input']
        output_cost = (usage.completion_tokens / 1000) * pricing['output']
        total_cost = input_cost + output_cost
        
        self.total_cost += total_cost
        self.call_history.append({
            'prompt_tokens': usage.prompt_tokens,
            'completion_tokens': usage.completion_tokens,
            'total_cost': total_cost
        })
        
        return {
            'prompt_tokens': usage.prompt_tokens,
            'completion_tokens': usage.completion_tokens,
            'input_cost': input_cost,
            'output_cost': output_cost,
            'total_cost': total_cost,
            'cumulative_cost': self.total_cost
        }
    
    def get_summary(self) -> Dict:
        """Get summary of all tracked costs"""
        return {
            'total_calls': len(self.call_history),
            'total_cost': self.total_cost,
            'avg_cost_per_call': self.total_cost / len(self.call_history) if self.call_history else 0,
            'call_history': self.call_history
        }


# Convenience functions for quick estimates
def estimate_mapping_cost(num_xml_nodes: int, 
                         num_target_columns: int,
                         num_historical_mappings: int = 50) -> float:
    """
    Quick cost estimate for mapping prediction
    
    Usage:
        cost = estimate_mapping_cost(100, 50, 25)
        print(f"Estimated cost: ${cost:.4f}")
    """
    estimator = CostEstimator()
    result = estimator.estimate_cost(
        num_xml_nodes,
        num_target_columns,
        num_historical_mappings
    )
    return result['total_cost']
