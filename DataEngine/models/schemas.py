from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import date


class DocumentChunk(BaseModel):
    """Модель фрагмента документа для векторного хранилища"""
    document_id: str = Field(description="Уникальный идентификатор исходного документа")
    chunk_number: int = Field(description="Последовательный номер фрагмента в документе")
    text: str = Field(description="Текстовое содержимое фрагмента")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Метаданные фрагмента (тип, статья, и т.д.)")
    
    def get_id(self) -> str:
        """Получение уникального идентификатора чанка"""
        return f"{self.document_id}_{self.chunk_number}" 

class LegalDocument(BaseModel):
    """Модель юридического документа"""
    file_path: str
    content: List[DocumentChunk] = Field(default_factory=list)
    doc_type: str = "unknown"
    adoption_date: Optional[date] = None
    keywords: List[str] = Field(default_factory=list)