import argparse
import json
import random
from pathlib import Path
from typing import List, Dict, Any
from tabulate import tabulate

from database.qdrant_client import QdrantManager
from utils.config_loader import load_config
from utils.logger import setup_logging, logger

def get_collection_info(qdrant: QdrantManager) -> Dict[str, Any]:
    """Получение основной информации о коллекции"""
    collection_info = qdrant.client.get_collection(qdrant.collection_name)
    return {
        "name": qdrant.collection_name,
        "vector_size": qdrant.vector_size,
        "total_points": collection_info.points_count,
        "vectors_config": {
            "size": collection_info.config.params.vectors.size,
            "distance": str(collection_info.config.params.vectors.distance)
        }
    }

def get_random_samples(qdrant: QdrantManager, count: int = 5) -> List[Dict]:
    """Получает случайные образцы данных из коллекции"""
    collection_info = qdrant.client.get_collection(qdrant.collection_name)
    total_points = collection_info.points_count
    
    if total_points == 0:
        logger.warning("Коллекция пуста")
        return []
    
    limit = min(count, total_points)
    offset = random.randint(0, max(1, total_points - limit))
    
    scroll_results = qdrant.client.scroll(
        collection_name=qdrant.collection_name,
        limit=limit,
        offset=offset,
        with_payload=True,
        with_vectors=True
    )[0]
    
    samples = []
    for point in scroll_results:
        metadata = point.payload.get("metadata", {})
        original_id = metadata.get("original_id", str(point.id))
        
        vector_preview = point.vector[:5] if point.vector else []
        
        samples.append({
            "id": point.id,
            "original_id": original_id,
            "text_preview": point.payload.get("text", "")[:100] + "..." if len(point.payload.get("text", "")) > 100 else point.payload.get("text", ""),
            "metadata": metadata,
            "vector_preview": vector_preview
        })
    
    return samples

def analyze_metadata_fields(qdrant: QdrantManager, sample_size: int = 100) -> Dict[str, Any]:
    """Анализирует и собирает статистику по полям метаданных"""
    scroll_results = qdrant.client.scroll(
        collection_name=qdrant.collection_name,
        limit=sample_size,
        with_payload=True,
        with_vectors=False
    )[0]
    
    metadata_fields = {}
    doc_types = {}
    article_numbers = []
    sections = []
    
    for point in scroll_results:
        metadata = point.payload.get("metadata", {})
        
        for key in metadata.keys():
            if key not in metadata_fields:
                metadata_fields[key] = 0
            metadata_fields[key] += 1
        
        doc_type = metadata.get("type", "unknown")
        if doc_type not in doc_types:
            doc_types[doc_type] = 0
        doc_types[doc_type] += 1
        
        if doc_type == "article" and "article_number" in metadata:
            article_numbers.append(metadata["article_number"])
        
        if doc_type == "section" and "title" in metadata:
            sections.append(metadata["title"])
    
    return {
        "metadata_fields": {
            "field_stats": metadata_fields,
            "top_fields": sorted(metadata_fields.items(), key=lambda x: x[1], reverse=True)[:10]
        },
        "doc_types": doc_types,
        "article_count": len(article_numbers),
        "unique_articles": len(set(article_numbers)),
        "section_count": len(sections),
        "unique_sections": len(set(sections))
    }

def format_output(data: Dict, format_type: str):
    """Форматирует вывод в различных форматах"""
    if format_type == "json":
        return json.dumps(data, ensure_ascii=False, indent=2)
    elif format_type == "table":
        tables = []
        
        if "name" in data:
            tables.append("\n=== Информация о коллекции ===")
            info_table = []
            for key, value in data.items():
                if key != "vectors_config":
                    info_table.append([key, value])
            tables.append(tabulate(info_table, headers=["Параметр", "Значение"], tablefmt="grid"))
        
        if "metadata_fields" in data:
            tables.append("\n=== Статистика полей метаданных ===")
            metadata_table = []
            for field, count in data["metadata_fields"]["top_fields"]:
                metadata_table.append([field, count])
            tables.append(tabulate(metadata_table, headers=["Поле", "Количество"], tablefmt="grid"))
            
            tables.append("\n=== Статистика типов документов ===")
            doc_types_table = []
            for doc_type, count in data["doc_types"].items():
                doc_types_table.append([doc_type, count])
            tables.append(tabulate(doc_types_table, headers=["Тип", "Количество"], tablefmt="grid"))
        
        if "samples" in data:
            tables.append("\n=== Образцы документов ===")
            for i, sample in enumerate(data["samples"]):
                tables.append(f"\nОбразец #{i+1} (ID: {sample['original_id']})")
                samples_table = []
                samples_table.append(["Текст", sample["text_preview"]])
                
                metadata = sample["metadata"]
                if "type" in metadata:
                    samples_table.append(["Тип", metadata["type"]])
                if "article_number" in metadata:
                    samples_table.append(["Номер статьи", metadata["article_number"]])
                if "title" in metadata:
                    samples_table.append(["Заголовок", metadata["title"]])
                
                tables.append(tabulate(samples_table, headers=["Поле", "Значение"], tablefmt="simple"))
        
        return "\n".join(tables)
    else:
        return str(data)

def parse_args():
    parser = argparse.ArgumentParser(description='Инспектирование данных в Qdrant')
    
    subparsers = parser.add_subparsers(dest='command', help='Команды')
    
    info_parser = subparsers.add_parser('info', help='Информация о коллекции')
    info_parser.add_argument('--format', choices=['json', 'table'], default='table',
                            help='Формат вывода')
    
    samples_parser = subparsers.add_parser('samples', help='Получить образцы данных')
    samples_parser.add_argument('--count', type=int, default=5,
                              help='Количество образцов')
    samples_parser.add_argument('--format', choices=['json', 'table'], default='table',
                              help='Формат вывода')
    
    metadata_parser = subparsers.add_parser('metadata', help='Анализ метаданных')
    metadata_parser.add_argument('--sample-size', type=int, default=100,
                               help='Размер выборки для анализа')
    metadata_parser.add_argument('--format', choices=['json', 'table'], default='table',
                               help='Формат вывода')
    
    analyze_parser = subparsers.add_parser('analyze', help='Полный анализ данных')
    analyze_parser.add_argument('--format', choices=['json', 'table'], default='table',
                              help='Формат вывода')
    analyze_parser.add_argument('--output', type=str, help='Путь для сохранения результатов')
    
    return parser.parse_args()

def main():
    args = parse_args()
    logger = setup_logging()
    logger.info("Запуск инспектирования Qdrant")
    
    try:
        config = load_config()
        logger.info("Конфигурация загружена успешно.")

        qdrant = QdrantManager(config.qdrant)
        logger.info("Qdrant клиент инициализирован.")
        
        result = {}
        
        if args.command == 'info':
            logger.info("Получение информации о коллекции")
            result = get_collection_info(qdrant)
            print(format_output(result, args.format))
            
        elif args.command == 'samples':
            logger.info(f"Получение {args.count} образцов данных")
            samples = get_random_samples(qdrant, args.count)
            result = {"samples": samples}
            print(format_output(result, args.format))
            
        elif args.command == 'metadata':
            logger.info(f"Анализ метаданных (выборка: {args.sample_size})")
            result = analyze_metadata_fields(qdrant, args.sample_size)
            print(format_output(result, args.format))
            
        elif args.command == 'analyze':
            logger.info("Запуск полного анализа данных")
            result = get_collection_info(qdrant)
            metadata_analysis = analyze_metadata_fields(qdrant, 100)
            samples = get_random_samples(qdrant, 3)
            
            result.update(metadata_analysis)
            result["samples"] = samples
            
            formatted_output = format_output(result, args.format)
            if args.output:
                output_path = Path(args.output)
                if args.format == 'json':
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(result, f, ensure_ascii=False, indent=2)
                else:
                    with open(output_path, 'w', encoding='utf-8') as f:
                        f.write(formatted_output)
                logger.info(f"Результаты сохранены в {output_path}")
            else:
                print(formatted_output)
                
        else:
            print("Укажите команду. Используйте --help для справки.")
            
    except Exception as e:
        logger.exception("Произошла критическая ошибка")
        print(f"Ошибка: {str(e)}")
        raise

if __name__ == "__main__":
    main() 