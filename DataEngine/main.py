import argparse
from typing import List
from pathlib import Path
import time
from data_processing.document_processor import DocumentProcessor
from data_processing.text_splitter import LegalTextSplitter
from database.qdrant_client import QdrantManager
from utils.config_loader import load_config
from utils.logger import setup_logging, logger
from embeddings.embedder import Embedder

def parse_args():
    parser = argparse.ArgumentParser(description='Process legal documents')
    
    subparsers = parser.add_subparsers(dest='mode', help='Режим работы')
    
    single_parser = subparsers.add_parser('single', help='Обработка одного документа')
    single_parser.add_argument('--document-type', 
                        choices=['legal-txt', 'pdf', 'html'],
                        default='legal-txt',
                        help='Type of documents to process')
    single_parser.add_argument('--input-file',
                        type=str,
                        required=True,
                        help='Path to input file')
    
    batch_parser = subparsers.add_parser('batch', help='Пакетная обработка документов')
    batch_parser.add_argument('--input-dir',
                        type=str,
                        default='data/documents',
                        help='Input directory with documents')
    batch_parser.add_argument('--extensions',
                        type=str,
                        nargs='+',
                        default=['.txt', '.pdf', '.html', '.xml'],
                        help='List of file extensions to process')
    batch_parser.add_argument('--stats',
                        action='store_true',
                        help='Generate processing statistics')
    
    reindex_parser = subparsers.add_parser('reindex', help='Очистка и переиндексация')
    reindex_parser.add_argument('--input-dir',
                        type=str,
                        default='data/documents',
                        help='Input directory with documents')
    reindex_parser.add_argument('--confirm',
                        action='store_true',
                        help='Confirm reindexing without prompt')
    
    return parser.parse_args()

def process_single_document(file_path: str, doc_type: str, processor, splitter, qdrant, embedder):
    """Обработка одного документа"""
    start_time = time.time()
    logger.info(f"Processing file: {file_path}")
    
    file_path = Path(file_path)
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return False
    
    document = processor.process_document(file_path, doc_type)
    if document:
        logger.info(f"Document processed successfully: {file_path}")
        chunks = splitter.split_document(document)
        logger.info(f"Document split into {len(chunks)} chunks.")
        embeddings = embedder.generate_embeddings([
            chunk.text for chunk in chunks
        ])
        logger.info("Embeddings generated for chunks.")
        qdrant.upsert_chunks(chunks, embeddings)
        logger.info(f"Chunks upserted to Qdrant for file: {file_path}")
        
        processing_time = time.time() - start_time
        logger.info(f"Document processing completed in {processing_time:.2f} seconds")
        return True
    else:
        logger.warning(f"Failed to process document: {file_path}")
        return False

def process_batch(input_dir: str, extensions: List[str], processor, splitter, qdrant, embedder, stats: bool = False):
    """Пакетная обработка документов"""
    input_dir = Path(input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        logger.error(f"Directory not found: {input_dir}")
        return
    
    start_time = time.time()
    logger.info(f"Batch processing documents from {input_dir}")
    
    results = processor.batch_process_documents(input_dir, extensions)
    
    total_chunks = 0
    for file_path in input_dir.glob("*"):
        if file_path.suffix.lower() in extensions:
            doc_type = 'legal-txt'
            if file_path.suffix.lower() == '.pdf':
                doc_type = 'pdf'
            elif file_path.suffix.lower() in ['.html', '.xml']:
                doc_type = 'html'
                
            document = processor.process_document(file_path, doc_type=doc_type)
            if document:
                chunks = splitter.split_document(document)
                total_chunks += len(chunks)
                
                if chunks:
                    embeddings = embedder.generate_embeddings([
                        chunk.text for chunk in chunks
                    ])
                    qdrant.upsert_chunks(chunks, embeddings)
    
    processing_time = time.time() - start_time
    logger.info(f"Batch processing completed in {processing_time:.2f} seconds")
    logger.info(f"Processed {results['processed_files']} files, "
               f"Failed: {results['failed_files']}, "
               f"Total chunks: {total_chunks}")
    
    if stats:
        print("\n=== Статистика обработки ===")
        print(f"Обработано файлов: {results['processed_files']}")
        print(f"Не удалось обработать: {results['failed_files']}")
        print(f"Всего чанков: {total_chunks}")
        print(f"Типы документов: {results['document_types']}")
        print(f"Общее время обработки: {processing_time:.2f} секунд")

def reindex_collection(input_dir: str, confirm: bool, processor, splitter, qdrant, embedder):
    """Очистка и переиндексация коллекции"""
    input_dir = Path(input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        logger.error(f"Directory not found: {input_dir}")
        return
    
    if not confirm:
        confirmation = input("Это действие удалит все существующие документы из базы. Продолжить? (y/n): ")
        if confirmation.lower() != 'y':
            logger.info("Reindexing cancelled by user")
            return
    
    logger.info("Recreating collection in Qdrant")
    qdrant._initialize_collection()
    
    logger.info("Starting reindexing process")
    process_batch(input_dir, ['.txt', '.pdf', '.html', '.xml'], processor, splitter, qdrant, embedder)
    logger.info("Reindexing completed")

def main():
    args = parse_args()
    logger = setup_logging()
    logger.info("Starting legal document processing")
    
    try:
        config = load_config()
        logger.info("Configuration loaded successfully.")

        processor = DocumentProcessor(config.processing.text_clean_patterns)
        logger.info("Document processor initialized.")

        splitter = LegalTextSplitter(
            chunk_size=config.processing.chunk_size,
            chunk_overlap=config.processing.chunk_overlap
        )
        logger.info("Legal text splitter initialized.")

        qdrant = QdrantManager(config.qdrant)
        logger.info("Qdrant client initialized.")

        embedder = Embedder(config.embeddings)
        logger.info("Embedder initialized.")

        if args.mode == 'single':
            process_single_document(
                args.input_file, 
                args.document_type, 
                processor, 
                splitter, 
                qdrant, 
                embedder
            )
        elif args.mode == 'batch':
            process_batch(
                args.input_dir, 
                args.extensions, 
                processor, 
                splitter, 
                qdrant, 
                embedder,
                args.stats
            )
        elif args.mode == 'reindex':
            reindex_collection(
                args.input_dir, 
                args.confirm, 
                processor, 
                splitter, 
                qdrant, 
                embedder
            )
        else:
            logger.info("No mode specified. Use --help for available options.")
            print("Укажите режим работы. Используйте --help для справки.")
            
    except Exception as e:
        logger.exception("Critical error occurred")
        raise

if __name__ == "__main__":
    main() 