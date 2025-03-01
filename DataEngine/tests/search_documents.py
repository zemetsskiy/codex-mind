import argparse
from database.qdrant_client import QdrantManager
from embeddings.embedder import Embedder
from search.document_search import LegalDocumentSearch
from utils.config_loader import load_config
from utils.logger import setup_logging

def format_search_result(result, max_text_length=300):
    """Форматирует результат поиска для вывода в терминал"""
    text = result['text']
    if len(text) > max_text_length:
        text = text[:max_text_length] + "..."
    
    metadata = result['metadata']
    doc_title = metadata.get('doc_title', metadata.get('title', 'Без названия'))
    
    if metadata.get('type') == 'article':
        article_num = metadata.get('article_number', '')
        return (f"\nСтатья {article_num} ({doc_title})\n"
                f"Релевантность: {result['score']:.2f}\n"
                f"Текст: {text}\n"
                f"ID: {result['id']}\n")
    else:
        return (f"\n{doc_title}\n"
                f"Релевантность: {result['score']:.2f}\n"
                f"Текст: {text}\n"
                f"ID: {result['id']}\n")

def parse_args():
    parser = argparse.ArgumentParser(description='Поиск юридических документов')
    
    subparsers = parser.add_subparsers(dest='command', help='Команды')
    
    search_parser = subparsers.add_parser('search', help='Поиск по запросу')
    search_parser.add_argument('query', type=str, help='Поисковый запрос')
    search_parser.add_argument('--limit', type=int, default=5, help='Количество результатов')
    search_parser.add_argument('--threshold', type=float, default=0.7, 
                              help='Минимальный порог релевантности (0-1)')
    
    keywords_parser = subparsers.add_parser('keywords', help='Поиск по ключевым словам')
    keywords_parser.add_argument('keywords', type=str, nargs='+', help='Ключевые слова')
    keywords_parser.add_argument('--limit', type=int, default=5, help='Количество результатов')
    
    return parser.parse_args()

def main():
    args = parse_args()
    logger = setup_logging()
    logger.info("Запуск системы поиска документов")
    
    try:
        config = load_config()
        logger.info("Конфигурация загружена успешно")

        qdrant = QdrantManager(config.qdrant)
        logger.info("Qdrant клиент инициализирован")

        embedder = Embedder(config.embeddings)
        logger.info("Embedder инициализирован")
        
        search_service = LegalDocumentSearch(embedder, qdrant)
        logger.info("Сервис поиска инициализирован")
        
        if args.command == 'search':
            print(f"\nПоиск по запросу: '{args.query}'")
            results = search_service.search(
                args.query, 
                limit=args.limit,
                threshold=args.threshold
            )
            
            if not results:
                print("\nНе найдено подходящих документов")
            else:
                print(f"\nНайдено результатов: {len(results)}")
                for result in results:
                    print(format_search_result(result))
                    
        elif args.command == 'keywords':
            print(f"\nПоиск по ключевым словам: {args.keywords}")
            results = search_service.search_by_keywords(
                args.keywords, 
                limit=args.limit
            )
            
            if not results:
                print("\nНе найдено подходящих документов")
            else:
                print(f"\nНайдено результатов: {len(results)}")
                for result in results:
                    print(format_search_result(result))

        else:
            print("\nУкажите команду. Используйте --help для справки")
            
    except Exception as e:
        logger.exception("Произошла критическая ошибка")
        print(f"Ошибка: {str(e)}")
        raise

if __name__ == "__main__":
    main() 