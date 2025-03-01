from typing import List, Dict, Any, Optional
from database.qdrant_client import QdrantManager
from embeddings.embedder import Embedder
from utils.logger import logger

class LegalDocumentSearch:
    """
    Сервис для поиска юридических документов с использованием 
    семантического поиска на базе векторных эмбеддингов
    """
    
    def __init__(self, embedder: Embedder, qdrant_manager: QdrantManager):
        """
        Инициализация сервиса поиска
        
        Args:
            embedder: Компонент для генерации эмбеддингов
            qdrant_manager: Клиент для работы с Qdrant
        """
        self.embedder = embedder
        self.qdrant = qdrant_manager
        logger.info("LegalDocumentSearch initialized")
        
    def search(self, query: str, limit: int = 5, threshold: float = 0.7) -> List[Dict[str, Any]]:
        """
        Поиск релевантных документов по естественному запросу
        
        Args:
            query: Текстовый запрос на естественном языке
            limit: Максимальное число результатов
            threshold: Минимальный порог релевантности
            
        Returns:
            Список найденных документов с метаданными и оценкой релевантности
        """
        logger.info(f"Searching for: '{query}'")
        
        # Генерация эмбеддинга для запроса
        query_embedding = self.embedder.generate_embeddings([query])[0]
        
        # Поиск похожих документов
        results = self.qdrant.search_similar(
            query_vector=query_embedding,
            limit=limit,
            threshold=threshold
        )
        
        logger.info(f"Found {len(results)} relevant document chunks")
        return results
    
    def search_by_keywords(self, keywords: List[str], limit: int = 5) -> List[Dict[str, Any]]:
        """
        Поиск документов по ключевым словам
        
        Args:
            keywords: Список ключевых слов
            limit: Максимальное число результатов
            
        Returns:
            Список найденных документов
        """
        # Объединяем ключевые слова в единый запрос
        query = " ".join(keywords)
        logger.info(f"Searching by keywords: {keywords}")
        
        return self.search(query, limit=limit, threshold=0.65)  # Используем меньший порог для keywords
    
    def find_related_documents(self, document_id: str, chunk_number: int, 
                              limit: int = 3) -> List[Dict[str, Any]]:
        """
        Поиск документов, связанных с конкретным документом/чанком
        
        Args:
            document_id: ID документа
            chunk_number: Номер чанка
            limit: Максимальное число результатов
            
        Returns:
            Список связанных документов
        """
        # Поиск по ID чанка
        chunk_id = f"{document_id}_{chunk_number}"
        search_results = self.qdrant.client.retrieve(
            collection_name=self.qdrant.collection_name,
            ids=[chunk_id]
        )
        
        if not search_results:
            logger.warning(f"Chunk with ID {chunk_id} not found")
            return []
        
        # Получаем вектор найденного чанка и ищем похожие, исключая сам чанк
        chunk_vector = search_results[0].vector
        results = self.qdrant.search_similar(
            query_vector=chunk_vector,
            limit=limit + 1,  # +1 чтобы исключить сам документ
            threshold=0.75
        )
        
        # Фильтруем, исключая сам исходный чанк
        filtered_results = [r for r in results if r["id"] != chunk_id]
        
        logger.info(f"Found {len(filtered_results)} related documents for {chunk_id}")
        return filtered_results[:limit] 