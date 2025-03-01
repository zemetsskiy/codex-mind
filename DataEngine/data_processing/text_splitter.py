from typing import List
import re
from utils.logger import logger
from models.schemas import LegalDocument, DocumentChunk

class LegalTextSplitter:
    """Специализированный разделитель текста для юридических документов"""
    
    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.sentence_endings = re.compile(r'(?<=[.!?])\s+')
        self.chunk_counter = 0  # Счетчик для генерации уникальных номеров чанков

    def split_document(self, document: LegalDocument) -> List[DocumentChunk]:
        try:
            chunks = []
            current_chunk = []
            current_length = 0
            
            # Сбрасываем счетчик для каждого нового документа
            self.chunk_counter = 0
            
            for element in document.content:
                if isinstance(element, DocumentChunk):
                    if element.metadata.get('type') == 'article':
                        chunks.extend(self._split_article(element, document.file_path))
                    else:
                        chunks.append(element)
            
            return chunks
        except Exception as e:
            logger.error(f"Error splitting document: {str(e)}")
            raise

    def _split_article(self, article: DocumentChunk, document_path: str) -> List[DocumentChunk]:
        """Разделение длинных статей на чанки с сохранением структуры"""
        chunks = []
        content = article.text
        sentences = self.sentence_endings.split(content)
        
        current_chunk = []
        current_length = 0
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            
            sentence_length = len(sentence)
            
            if current_length + sentence_length > self.chunk_size:
                chunks.append(self._create_chunk(current_chunk, article.metadata, article.document_id))
                current_chunk = current_chunk[-self.chunk_overlap:]
                current_length = sum(len(s) for s in current_chunk)
            
            current_chunk.append(sentence)
            current_length += sentence_length
        
        if current_chunk:
            chunks.append(self._create_chunk(current_chunk, article.metadata, article.document_id))
        
        return chunks

    def _create_chunk(self, sentences: List[str], metadata: dict, document_id: str) -> DocumentChunk:
        """Создает новый чанк с правильными метаданными и ID"""
        self.chunk_counter += 1
        
        return DocumentChunk(
            document_id=document_id,
            chunk_number=self.chunk_counter,
            text=' '.join(sentences),
            metadata={
                **metadata,
                'chunk_type': 'article_part',
                'items': self._extract_items(' '.join(sentences))
            }
        )

    def _extract_items(self, text: str) -> List[dict]:
        items = []
        for match in re.finditer(r'(\d+\.)\s+(.*?)(?=\d+\.|$)', text):
            items.append({
                'number': match.group(1).strip(),
                'text': match.group(2).strip()
            })
        return items 