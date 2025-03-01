import sys
import json
import random
from typing import List, Dict, Any
from qdrant_client import QdrantClient
from qdrant_client.http import models

QDRANT_HOST = "localhost"
QDRANT_PORT = 6333

def get_client() -> QdrantClient:
    """Инициализирует и возвращает клиент Qdrant"""
    return QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

def list_collections() -> List[str]:
    """Получает список всех коллекций"""
    client = get_client()
    collections = client.get_collections()
    return [collection.name for collection in collections.collections]

def get_collection_info(collection_name: str) -> Dict[str, Any]:
    """Получает информацию о коллекции"""
    client = get_client()
    try:
        collection = client.get_collection(collection_name)
        return {
            "name": collection_name,
            "vectors_config": {
                "size": collection.config.params.vectors.size,
                "distance": str(collection.config.params.vectors.distance)
            },
            "points_count": collection.points_count,
            "indexed_vectors": collection.vectors_count
        }
    except Exception as e:
        return {"error": str(e)}

def get_random_points(collection_name: str, count: int = 5) -> List[Dict]:
    """Получает случайные точки из коллекции"""
    client = get_client()
    
    try:
        # Получаем общее количество точек
        collection_info = client.get_collection(collection_name)
        total_points = collection_info.points_count
        
        if total_points == 0:
            return []
        
        # Получаем случайное смещение
        offset = random.randint(0, max(1, total_points - count))
        
        # Получаем точки
        points, _ = client.scroll(
            collection_name=collection_name,
            offset=offset,
            limit=min(count, total_points),
            with_payload=True
        )
        
        # Форматируем результат
        result = []
        for point in points:
            point_data = {
                "id": point.id,
                "payload": point.payload
            }
            result.append(point_data)
            
        return result
    except Exception as e:
        return [{"error": str(e)}]

def count_by_filter(collection_name: str, filter_field: str, filter_value: str) -> int:
    """Подсчет точек по фильтру"""
    client = get_client()
    
    try:
        # Создаем фильтр
        filter_query = models.Filter(
            must=[
                models.FieldCondition(
                    key=f"metadata.{filter_field}",
                    match=models.MatchValue(value=filter_value)
                )
            ]
        )
        
        # Получаем количество точек
        count = client.count(
            collection_name=collection_name,
            count_filter=filter_query
        )
        
        return count.count
    except Exception as e:
        print(f"Ошибка: {str(e)}")
        return 0

def perform_search(collection_name: str, query_vector: List[float], limit: int = 5):
    """Выполняет поиск по вектору"""
    client = get_client()
    
    try:
        results = client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            limit=limit
        )
        
        # Форматируем результат
        search_results = []
        for result in results:
            search_results.append({
                "id": result.id,
                "score": result.score,
                "payload": result.payload
            })
            
        return search_results
    except Exception as e:
        return [{"error": str(e)}]

def main():
    if len(sys.argv) < 2:
        print("Укажите команду")
        return
    
    command = sys.argv[1]
    
    if command == "collections":
        # Получаем список коллекций
        collections = list_collections()
        print(json.dumps(collections, indent=2))
    
    elif command == "info" and len(sys.argv) >= 3:
        # Получаем информацию о коллекции
        collection_name = sys.argv[2]
        info = get_collection_info(collection_name)
        print(json.dumps(info, indent=2))
    
    elif command == "count" and len(sys.argv) >= 3:
        # Получаем количество точек
        collection_name = sys.argv[2]
        info = get_collection_info(collection_name)
        print(f"Количество точек в коллекции {collection_name}: {info.get('points_count', 0)}")
    
    elif command == "sample" and len(sys.argv) >= 4:
        # Получаем случайные точки
        collection_name = sys.argv[2]
        count = int(sys.argv[3])
        points = get_random_points(collection_name, count)
        print(json.dumps(points, indent=2))
    
    elif command == "filter" and len(sys.argv) >= 5:
        # Подсчет по фильтру
        collection_name = sys.argv[2]
        filter_field = sys.argv[3]
        filter_value = sys.argv[4]
        count = count_by_filter(collection_name, filter_field, filter_value)
        print(f"Количество точек с {filter_field}={filter_value}: {count}")
    
    else:
        print("Неизвестная команда или недостаточно аргументов")
        print("Использование:")
        print("  python qdrant_utility.py collections")
        print("  python qdrant_utility.py info <collection_name>")
        print("  python qdrant_utility.py count <collection_name>")
        print("  python qdrant_utility.py sample <collection_name> <count>")
        print("  python qdrant_utility.py filter <collection_name> <field> <value>")

if __name__ == "__main__":
    main() 