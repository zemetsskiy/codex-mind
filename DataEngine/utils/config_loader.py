from pathlib import Path
import yaml
from pydantic import BaseModel

class QdrantConfig(BaseModel):
    host: str
    port: int
    collection_name: str
    vector_size: int

class ProcessingConfig(BaseModel):
    chunk_size: int
    chunk_overlap: int
    allowed_extensions: list[str]
    text_clean_patterns: list[str]
    legal_text: dict = {}

class EmbeddingsConfig(BaseModel):
    model_name: str
    device: str
    batch_size: int

class AppConfig(BaseModel):
    qdrant: QdrantConfig
    processing: ProcessingConfig
    embeddings: EmbeddingsConfig

def load_config(config_path: Path = Path("config/config.yaml")) -> AppConfig:
    with open(config_path) as f:
        config_data = yaml.safe_load(f)
    return AppConfig(**config_data) 