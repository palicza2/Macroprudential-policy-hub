"""
RAG (Retrieval-Augmented Generation) Retriever for Knowledge Graph.
Converts graph data to searchable text chunks for LLM context retrieval.
"""
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class KnowledgeGraphRAG:
    """
    RAG retriever for knowledge graph data.
    Converts graph nodes and edges to text chunks for semantic search.
    """
    
    def __init__(self, graph_data: Optional[Dict[str, Any]] = None):
        """
        Initialize RAG retriever with graph data.
        
        Args:
            graph_data: Knowledge graph data with 'nodes' and 'edges' keys
        """
        self.graph_data = graph_data or {'nodes': [], 'edges': []}
        self._chunks = None
        self._build_chunks()
    
    def _build_chunks(self):
        """Build text chunks from graph data."""
        chunks = []
        nodes = self.graph_data.get('nodes', [])
        edges = self.graph_data.get('edges', [])
        
        # Node chunks
        for node in nodes:
            node_type = node.get('group', 'unknown')
            label = node.get('label', '')
            node_id = node.get('id', '')
            
            if node_type == 'country':
                chunk = f"Country: {label} (ISO2: {node_id})"
                if node.get('region'):
                    chunk += f", Region: {node.get('region')}"
                if node.get('value'):
                    chunk += f", Total Capital Buffer: {node.get('value'):.2f}%"
                chunks.append({
                    'text': chunk,
                    'type': 'country',
                    'node_id': node_id,
                    'metadata': node
                })
            
            elif node_type in ['ccyb', 'syrb', 'osii']:
                chunk = f"{node_type.upper()}: {label}"
                if node.get('value'):
                    chunk += f", Rate: {node.get('value'):.2f}%"
                chunks.append({
                    'text': chunk,
                    'type': node_type,
                    'node_id': node_id,
                    'metadata': node
                })
            
            elif node_type == 'bbm':
                chunk = f"Borrower-Based Measure: {label}"
                chunks.append({
                    'text': chunk,
                    'type': 'bbm',
                    'node_id': node_id,
                    'metadata': node
                })
        
        # Edge chunks (relationships)
        for edge in edges:
            edge_label = edge.get('label', '')
            from_node = edge.get('from', '')
            to_node = edge.get('to', '')
            
            # Find node labels
            from_label = next((n.get('label', from_node) for n in nodes if n.get('id') == from_node), from_node)
            to_label = next((n.get('label', to_node) for n in nodes if n.get('id') == to_node), to_node)
            
            if edge_label == 'HAS':
                chunk = f"{from_label} has {to_label}"
            elif edge_label == 'SIMILAR':
                chunk = f"{from_label} is similar to {to_label} (similar capital buffer levels)"
            elif edge_label == 'SIMILAR_MEASURE':
                chunk = f"{from_label} and {to_label} are similar measures (similar rates)"
            elif edge_label == 'COEXISTS':
                chunk = f"{from_label} and {to_label} coexist in the same country"
            else:
                chunk = f"{from_label} {edge_label.lower()} {to_label}"
            
            chunks.append({
                'text': chunk,
                'type': 'relationship',
                'edge_label': edge_label,
                'from': from_node,
                'to': to_node,
                'metadata': edge
            })
        
        self._chunks = chunks
        logger.debug(f"Built {len(chunks)} text chunks from knowledge graph")
    
    def retrieve_context(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """
        Retrieve relevant graph context for a query.
        
        Args:
            query: Search query string
            top_k: Number of top results to return
        
        Returns:
            List of relevant chunks with metadata
        """
        if not self._chunks:
            return []
        
        query_lower = query.lower()
        scored_chunks = []
        
        # Simple keyword-based scoring (can be enhanced with embeddings later)
        for chunk in self._chunks:
            text = chunk.get('text', '').lower()
            score = 0
            
            # Exact match
            if query_lower in text:
                score += 10
            
            # Word overlap
            query_words = set(query_lower.split())
            text_words = set(text.split())
            overlap = len(query_words & text_words)
            score += overlap * 2
            
            # Type-specific boosting
            if 'country' in query_lower and chunk.get('type') == 'country':
                score += 5
            if any(term in query_lower for term in ['ccyb', 'syrb', 'osii', 'bbm']) and chunk.get('type') in ['ccyb', 'syrb', 'osii', 'bbm']:
                score += 5
            if 'similar' in query_lower and chunk.get('edge_label') == 'SIMILAR':
                score += 5
            
            if score > 0:
                scored_chunks.append((score, chunk))
        
        # Sort by score and return top_k
        scored_chunks.sort(key=lambda x: x[0], reverse=True)
        return [chunk for _, chunk in scored_chunks[:top_k]]
    
    def get_country_context(self, country_name: str) -> List[Dict[str, Any]]:
        """
        Get all context related to a specific country.
        
        Args:
            country_name: Country name or ISO2 code
        
        Returns:
            List of relevant chunks
        """
        return self.retrieve_context(country_name, top_k=20)
    
    def get_measure_context(self, measure_type: str) -> List[Dict[str, Any]]:
        """
        Get all context related to a specific measure type.
        
        Args:
            measure_type: 'ccyb', 'syrb', 'osii', or 'bbm'
        
        Returns:
            List of relevant chunks
        """
        return self.retrieve_context(measure_type, top_k=20)
    
    def update_graph_data(self, graph_data: Dict[str, Any]):
        """
        Update graph data and rebuild chunks.
        
        Args:
            graph_data: New knowledge graph data
        """
        self.graph_data = graph_data
        self._build_chunks()
