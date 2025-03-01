from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from typing import List
import time
import hashlib
from models.schemas import DocumentChunk
from utils.logger import logger

class QdrantManager:
    def __init__(self, config):
        self.client = QdrantClient(
            host=config.host,
            port=config.port,
            timeout=60
        )
        self.collection_name = config.collection_name
        self.vector_size = config.vector_size
        self.id_mapping = {}
        
        self._initialize_collection()

    def _initialize_collection(self):
        """Инициализация коллекции если не существует"""
        if not self.client.collection_exists(self.collection_name):
            self.client.recreate_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=self.vector_size,
                    distance=Distance.COSINE
                )
            )
            logger.info(f"Created new collection: {self.collection_name}")

    def _convert_string_id_to_numeric(self, string_id: str) -> int:
        hash_object = hashlib.sha256(string_id.encode())
        hex_dig = hash_object.hexdigest()
        numeric_id = int(hex_dig[-16:], 16)        
        self.id_mapping[numeric_id] = string_id
        
        return numeric_id

    def upsert_chunks(self, chunks: List[DocumentChunk], embeddings: List[List[float]], batch_size: int = 100):
        """Вставка или обновление чанков в базе с разбивкой на пакеты"""
        total_chunks = len(chunks)
        
        if total_chunks == 0:
            logger.warning("No chunks to upsert")
            return
        
        logger.info(f"Upserting {total_chunks} chunks in batches of {batch_size}")
        
        points = []
        for chunk, embedding in zip(chunks, embeddings):
            string_id = f"{chunk.document_id}_{chunk.chunk_number}"
            numeric_id = self._convert_string_id_to_numeric(string_id)
            
            metadata = chunk.metadata.copy() if chunk.metadata else {}
            metadata["original_id"] = string_id
            
            points.append(PointStruct(
                id=numeric_id,
                vector=embedding,
                payload={
                    "text": chunk.text,
                    "metadata": metadata
                }
            ))
        
        batches = [points[i:i + batch_size] for i in range(0, len(points), batch_size)]
        logger.info(f"Split into {len(batches)} batches")
        
        total_inserted = 0
        for i, batch in enumerate(batches):
            try:
                logger.info(f"Upserting batch {i+1}/{len(batches)} with {len(batch)} chunks")
                operation_info = self.client.upsert(
                    collection_name=self.collection_name,
                    wait=True,
                    points=batch
                )
                total_inserted += len(batch)
                logger.info(f"Batch {i+1} inserted successfully. Status: {operation_info.status}")
                
                if i < len(batches) - 1:
                    time.sleep(0.2)
                    
            except Exception as e:
                logger.error(f"Error upserting batch {i+1}: {str(e)}")
                if len(batch) > 10:
                    logger.info(f"Retrying batch {i+1} with smaller chunks")
                    smaller_batches = [batch[j:j + batch_size//2] for j in range(0, len(batch), batch_size//2)]
                    for k, small_batch in enumerate(smaller_batches):
                        try:
                            operation_info = self.client.upsert(
                                collection_name=self.collection_name,
                                wait=True,
                                points=small_batch
                            )
                            total_inserted += len(small_batch)
                            logger.info(f"Small batch {k+1}/{len(smaller_batches)} inserted successfully")
                            time.sleep(0.3)
                        except Exception as e2:
                            logger.error(f"Failed to insert small batch {k+1}: {str(e2)}")
                else:
                    logger.error(f"Failed to insert chunks in batch {i+1}, skipping")
        
        logger.info(f"Inserted {total_inserted}/{total_chunks} chunks total")

    def search_similar(self, query_vector: List[float], limit: int = 5, threshold: float = 0.7):
        """Поиск похожих документов на основе векторного запроса"""
        search_results = self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            limit=limit,
            score_threshold=threshold
        )
        
        results = []
        for result in search_results:
            metadata = result.payload.get("metadata", {})
            original_id = metadata.get("original_id", str(result.id))
            
            results.append({
                "text": result.payload.get("text", ""),
                "metadata": metadata,
                "score": result.score,
                "id": original_id
            })
            
        logger.info(f"Found {len(results)} similar documents with threshold {threshold}")
        return results
        
    def delete_document(self, document_id: str):
        """Удаляет все чанки, принадлежащие документу с заданным ID"""
        filter_query = {
            "must": [
                {
                    "key": "metadata.original_id",
                    "match": {
                        "text": f"{document_id}_"
                    }
                }
            ]
        }
        
        scroll_results = self.client.scroll(
            collection_name=self.collection_name,
            scroll_filter=filter_query,
            limit=10000,
            with_payload=False,
            with_vectors=False
        )[0]
        
        if not scroll_results:
            logger.warning(f"No chunks found for document {document_id}")
            return
        
        ids_to_delete = [point.id for point in scroll_results]
        
        if ids_to_delete:
            operation_info = self.client.delete(
                collection_name=self.collection_name,
                points_selector=ids_to_delete,
                wait=True
            )
            
            logger.info(f"Deleted {len(ids_to_delete)} chunks for document {document_id}. Status: {operation_info.status}")
        else:
            logger.warning(f"No chunks to delete for document {document_id}") 