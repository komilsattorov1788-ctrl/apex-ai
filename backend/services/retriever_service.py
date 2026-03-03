import numpy as np
import logging

try:
    import faiss
    from sentence_transformers import SentenceTransformer
    HAS_RAG = True
except ImportError:
    HAS_RAG = False

logger = logging.getLogger("neural_sync.retriever")

class KnowledgeRetriever:
    """
    HIGH-IMPACT: Vector DB & RAG Integration
    Embeds documents and retrieves top-K context to ground LLM generations, reducing hallucinations.
    """
    def __init__(self):
        if HAS_RAG:
            self.model = SentenceTransformer('all-MiniLM-L6-v2')
            self.dimension = 384
            self.index = faiss.IndexFlatL2(self.dimension)
            self.documents = []
        else:
            logger.warning("FAISS or SentenceTransformers not installed. RAG degraded to mock.")
    
    def add_document(self, text: str, doc_id: str):
        if not HAS_RAG: return
        vector = self.model.encode([text])
        self.index.add(np.array(vector).astype('float32'))
        self.documents.append({"id": doc_id, "content": text})
        
    def get_context(self, query: str, top_k: int = 3) -> str:
        if not HAS_RAG or getattr(self, 'index', None) is None or self.index.ntotal == 0:
            return ""
        
        vector = self.model.encode([query]).astype('float32')
        distances, indices = self.index.search(vector, top_k)
        
        context_parts = []
        for i, idx in enumerate(indices[0]):
            if idx != -1 and idx < len(self.documents):
                context_parts.append(self.documents[idx]['content'])
                
        return "\n---\n".join(context_parts)

retriever = KnowledgeRetriever()
