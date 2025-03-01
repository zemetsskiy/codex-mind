from typing import List
from sentence_transformers import SentenceTransformer
import torch

class Embedder:
    def __init__(self, config):
        self.device = config.device if hasattr(config, 'device') else 'cpu'
        self.model = SentenceTransformer(
            config.model_name,
            device=self.device
        )
        self.batch_size = config.batch_size

    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        return self.model.encode(
            texts,
            batch_size=self.batch_size,
            convert_to_tensor=False,
            normalize_embeddings=True
        ).tolist() 