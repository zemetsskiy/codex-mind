import re
from pathlib import Path
from typing import List, Optional
import textract
from bs4 import BeautifulSoup
import PyPDF2
from models.schemas import LegalDocument, DocumentChunk
from utils.logger import logger
from datetime import datetime

class DocumentProcessor:    
    def __init__(self, clean_patterns: List[str]):
        self.clean_patterns = [re.compile(p) for p in clean_patterns]
        self.legal_processor = LegalTextProcessor(clean_patterns)
        self.document_counter = 0

    def _clean_text(self, text: str) -> str:
        """Очистка текста от шумов с использованием регулярных выражений"""
        for pattern in self.clean_patterns:
            text = pattern.sub('', text)
        return text.strip()

    def _process_pdf(self, file_path: Path) -> str:
        text = ""
        with open(file_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            for page in reader.pages:
                text += page.extract_text() + "\n"
        return self._clean_text(text)

    def _process_html_xml(self, file_path: Path) -> str:
        with open(file_path, 'r', encoding='cp1251') as f:
            soup = BeautifulSoup(f, 'lxml')
        logger.info(f"Read HTML/XML file {file_path} with cp1251 encoding")
            
        for elem in soup(['script', 'style', 'header', 'footer']):
            elem.decompose()
            
        structure = []
        for tag in soup.find_all(['h1', 'h2', 'h3', 'article', 'section']):
            structure.append(tag.get_text(strip=True, separator=' '))
            
        return self._clean_text('\n'.join(structure))

    def process_document(self, file_path: Path, doc_type: str = 'legal-txt') -> Optional[LegalDocument]:
        try:
            self.document_counter += 1
            document_id = f"doc_{self.document_counter}_{file_path.stem}"
            
            if doc_type == 'legal-txt' and file_path.suffix.lower() == '.txt':
                return self._process_legal_txt(file_path, document_id)
            elif file_path.suffix.lower() == '.pdf':
                content = self._process_pdf(file_path)
                return self._create_document(file_path, content, document_id)
            elif file_path.suffix.lower() in ('.html', '.xml'):
                content = self._process_html_xml(file_path)
                return self._create_document(file_path, content, document_id)
            else:
                try:
                    content = textract.process(str(file_path)).decode('cp1251', errors='replace')
                except UnicodeDecodeError:
                    content = textract.process(str(file_path)).decode('utf-8', errors='replace')
                return self._create_document(file_path, content, document_id)
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}", exc_info=True)
            return None

    def _create_document(self, file_path: Path, content: str, document_id: str) -> LegalDocument:
        metadata = self._extract_document_metadata(content)
        
        chunks = []
        max_chunk_size = 1000
        
        for i in range(0, len(content), max_chunk_size):
            chunk_text = content[i:i+max_chunk_size]
            chunks.append(DocumentChunk(
                document_id=document_id,
                chunk_number=i//max_chunk_size + 1,
                text=chunk_text,
                metadata={
                    'type': 'text_fragment',
                    'title': metadata.get('title', ''),
                    'doc_type': metadata.get('type', 'unknown'),
                    'offset': i
                }
            ))
        
        return LegalDocument(
            file_path=str(file_path),
            content=chunks,
            doc_type=metadata.get('type', 'law'),
            adoption_date=metadata.get('adoption_date'),
            keywords=metadata.get('keywords', [])
        )

    def _process_legal_txt(self, file_path: Path, document_id: str) -> LegalDocument:
        """Специфичная обработка юридических текстовых файлов в кодировке cp1251"""
        try:
            with open(file_path, 'r', encoding='cp1251') as f:
                raw_content = f.read()
            logger.info(f"Read file {file_path} with cp1251 encoding")
        except UnicodeDecodeError:
            logger.warning(f"Failed to read with cp1251, trying utf-8 as fallback for {file_path}")
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                raw_content = f.read()
        
        chunks = self.legal_processor.process_legal_text(raw_content)
        logger.info(f"Generated {len(chunks)} chunks from {file_path}")
        
        metadata = self._extract_document_metadata(raw_content)
        
        return LegalDocument(
            file_path=str(file_path),
            content=chunks,
            doc_type=metadata.get('type', 'law'),
            adoption_date=metadata.get('adoption_date'),
            keywords=metadata.get('keywords', [])
        )

    def _extract_document_metadata(self, text: str) -> dict:
        """Извлечение метаданных из текста документа"""
        metadata = {}
        
        title_match = re.search(
            r'"(.*?)"\s+от\s+(\d{2}\.\d{2}\.\d{4})', 
            text
        )
        if title_match:
            metadata['title'] = title_match.group(1)
            try:
                metadata['adoption_date'] = datetime.strptime(
                    title_match.group(2), 
                    '%d.%m.%Y'
                ).date()
            except ValueError:
                logger.warning(f"Could not parse date from: {title_match.group(2)}")
        
        if 'Гражданский кодекс' in text:
            metadata['type'] = 'codex'
        elif 'Федеральный закон' in text:
            metadata['type'] = 'federal_law'
        else:
            #TODO: Добавить другие типы документов
            pass
        
        edition_match = re.search(
            r'ред\. от (\d{2}\.\d{2}\.\d{4})', 
            text
        )
        if edition_match:
            try:
                metadata['last_edition'] = datetime.strptime(
                    edition_match.group(1), 
                    '%d.%m.%Y'
                ).date()
            except ValueError:
                logger.warning(f"Could not parse edition date from: {edition_match.group(1)}")
        
        return metadata

    def batch_process_documents(self, directory_path: Path, extensions: List[str] = None) -> dict:
        """Пакетная обработка документов из указанной директории"""
        if not directory_path.exists() or not directory_path.is_dir():
            logger.error(f"Directory {directory_path} does not exist or is not a directory")
            raise ValueError(f"Invalid directory path: {directory_path}")
        
        if extensions is None:
            extensions = ['.txt', '.pdf', '.html', '.xml', '.doc', '.docx']
        
        stats = {
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'document_types': {},
            'total_chunks': 0
        }
        
        files_to_process = []
        for extension in extensions:
            files_to_process.extend(directory_path.glob(f"*{extension}"))
        
        for file_path in files_to_process:
            logger.info(f"Processing file: {file_path}")
            try:
                doc_type = 'legal-txt'
                if file_path.suffix.lower() == '.pdf':
                    doc_type = 'pdf'
                elif file_path.suffix.lower() in ['.html', '.xml']:
                    doc_type = 'html'
                
                document = self.process_document(file_path, doc_type)
                
                if document:
                    stats['processed_files'] += 1
                    
                    doc_type = document.doc_type
                    if doc_type in stats['document_types']:
                        stats['document_types'][doc_type] += 1
                    else:
                        stats['document_types'][doc_type] = 1
                    
                    stats['total_chunks'] += len(document.content)
                else:
                    stats['failed_files'] += 1
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}", exc_info=True)
                stats['failed_files'] += 1
        
        logger.info(f"Batch processing completed. Processed: {stats['processed_files']} files, "
                   f"Failed: {stats['failed_files']} files, "
                   f"Total chunks: {stats['total_chunks']}")
        
        return stats
        
    def extract_document_citations(self, content: str) -> List[dict]:
        """Извлекает упоминания других нормативных актов в документе"""
        citations = []
        
        patterns = [
            (r'(?:Федеральн(?:ый|ого|ому) закон(?:а|у|ом)?)\s+"([^"]+)"\s+от\s+(\d{1,2}\.\d{1,2}\.\d{4})\s+[NН]\s+(\d+-[А-Я]+)',
             'federal_law'),
            
            (r'([А-Я][а-я]+(?:ом|ого|ий|ый))\s+кодекс(?:а|е|ом|у)?\s+Российской\s+Федерации', 
             'codex'),
            
            (r'Постановлени(?:е|я|ю|ем)\s+Правительства\s+Российской\s+Федерации\s+от\s+(\d{1,2}\.\d{1,2}\.\d{4})\s+[NН]\s+(\d+)', 
             'government_decree'),
            
            (r'Приказ(?:а|е|ом|у)?\s+(?:Министерства|Минфина|Минюста|ФНС)[^"]*\s+от\s+(\d{1,2}\.\d{1,2}\.\d{4})\s+[NН]\s+(\d+)', 
             'ministry_order')
        ]
        
        for pattern, doc_type in patterns:
            matches = re.finditer(pattern, content, re.IGNORECASE)
            for match in matches:
                citation = {
                    'type': doc_type,
                    'match': match.group(0),
                    'groups': match.groups()
                }
                citations.append(citation)
        
        return citations

class LegalTextProcessor:
    """Обработчик текстовых документов юридического содержания"""
    
    SECTION_PATTERN = re.compile(
        r'(Раздел|Глава|Подраздел)\s+([IVXLCDM\d]+)?\.?\s*(.*?)(?=(?:Раздел|Глава|Подраздел|Статья)\s+|$)',
        re.DOTALL | re.IGNORECASE
    )
    
    ARTICLE_PATTERN = re.compile(
        r'(Статья\s+\d+(?:[\.\d]*))\.\s*(.*?)(?=(?:Статья\s+\d+|Раздел|Глава|Подраздел|$))', 
        re.DOTALL | re.IGNORECASE
    )
    
    ITEM_PATTERN = re.compile(
        r'(?m)^(\d+(?:\.\d+)*)\.\s+(.*?)(?=(?:^\d+(?:\.\d+)*\.\s+|\n\n|$))',
        re.DOTALL
    )

    SUBITEM_PATTERN = re.compile(
        r'([а-я])\)\s+(.*?)(?=(?:[а-я]\)\s+|$))',
        re.DOTALL
    )

    def __init__(self, clean_patterns: list[str]):
        self.clean_patterns = [re.compile(p) for p in clean_patterns]
        self.line_number_pattern = re.compile(r'^\s*\d+\|', re.MULTILINE)
        self.document_counter = 0

    def process_legal_text(self, content: str) -> list[DocumentChunk]:
        """Основной метод обработки юридического текста"""
        content = self._preprocess_text(content)
        self.document_counter += 1
        document_id = f"doc_{self.document_counter}"
        return self._structure_document(content, document_id)

    def _preprocess_text(self, text: str) -> str:
        """Предварительная очистка текста"""
        # Удаление номеров строк и технической информации
        text = self.line_number_pattern.sub('', text)
        
        # Нормализация переносов строк и пробелов
        text = re.sub(r'\r\n|\r|\n', '\n', text)  # Нормализация переносов строк
        text = re.sub(r'([^\n])\n([^\n])', r'\1 \2', text)  # Объединение разорванных строк
        
        # Очистка заголовков от нумерации страниц и технических разделителей
        text = re.sub(r'--+', '', text)
        text = re.sub(r'\f|\v', '\n', text)  # Замена form feed и vertical tab на новую строку
        
        # Очистка специфичных символов
        text = re.sub(r'\xad', '', text)  # Удаление мягких переносов
        text = re.sub(r'\xa0', ' ', text)  # Замена неразрывных пробелов
        text = re.sub(r'["""]', '"', text)  # Нормализация кавычек
        
        # Удаление технической информации о документе
        text = re.sub(
            r'(?:Документ предоставлен|Дата сохранения|КонсультантПлюс|www\.consultant\.ru).*?\n',
            '', 
            text, 
            flags=re.DOTALL | re.IGNORECASE
        )
        
        # Удаление информации о дате создания документа
        text = re.sub(
            r'\d{1,2}\s+[а-яА-Я]+\s+\d{4}\s+года\s+[NН]\s+\d+(?:-[А-Я]+)?\s*\n',
            '', 
            text
        )
        
        # Очистка префиксов статей для лучшего соответствия шаблонам
        text = re.sub(r'(Статья\s+\d+)\s*\.', r'\1.', text)
        
        # Обеспечиваем отступы перед заголовками разделов и статей
        text = re.sub(r'([^\n])(Раздел|Глава|Подраздел|Статья)', r'\1\n\2', text)
        
        # Нормализация пробелов (убираем множественные пробелы)
        text = re.sub(r'\s+', ' ', text)
        
        # Восстановление переносов строк для структурных элементов
        text = re.sub(r' (?=Статья\s+\d|Раздел|Глава|Подраздел)', '\n', text)
        text = re.sub(r'(?<=\.)(\d+)\.\s+', r'.\n\1. ', text)  # Разделение пунктов
        
        # Двойной перенос перед разделами и статьями для лучшего разделения
        text = re.sub(r'\n(Раздел|Глава|Подраздел)', r'\n\n\1', text)
        text = re.sub(r'\n(Статья\s+\d)', r'\n\n\1', text)
        
        text = text.strip()
        
        return text

    def _structure_document(self, text: str, document_id: str) -> list[DocumentChunk]:
        """Структурирование документа на иерархические блоки"""
        chunks = []
        chunk_counter = 0
        current_section = None
        
        doc_metadata = self._extract_document_metadata(text)
        
        for section_match in self.SECTION_PATTERN.finditer(text):
            try:
                groups = section_match.groups()
                logger.debug(f"Section match groups: {groups}")
                
                section_type = groups[0].strip()
                section_number = groups[1].strip() if groups[1] else ''
                section_title = groups[2].strip()
                section_text = section_match.group(0).strip()
                
                chunk_counter += 1
                current_section = {
                    'type': section_type,
                    'number': section_number,
                    'title': section_title
                }
                
                chunks.append(DocumentChunk(
                    document_id=document_id,
                    chunk_number=chunk_counter,
                    text=section_text,
                    metadata={
                        'type': 'section',
                        'section_type': section_type,
                        'section_number': section_number,
                        'title': section_title,
                        'doc_title': doc_metadata.get('title', ''),
                        'doc_date': doc_metadata.get('adoption_date', '')
                    }
                ))
                logger.debug(f"Created section chunk: {section_type} {section_number}")
            except Exception as e:
                logger.warning(f"Error processing section: {e}", exc_info=True)
                continue
        
        for article_match in self.ARTICLE_PATTERN.finditer(text):
            try:
                article_parts = article_match.groups()
                
                if len(article_parts) >= 2:
                    article_number = article_parts[0].strip()
                    article_content = article_parts[1].strip()
                    article_text = article_match.group(0).strip()
                    
                    # Извлечение пунктов и подпунктов статьи
                    items = []
                    for item_match in self.ITEM_PATTERN.finditer(article_content):
                        item_number = item_match.group(1).strip()
                        item_text = item_match.group(2).strip()
                        
                        # Поиск подпунктов
                        subitems = []
                        for subitem_match in self.SUBITEM_PATTERN.finditer(item_text):
                            subitems.append({
                                'number': subitem_match.group(1).strip(),
                                'text': subitem_match.group(2).strip()
                            })
                        
                        items.append({
                            'number': item_number,
                            'text': item_text,
                            'subitems': subitems
                        })
                    
                    # Создание чанка для статьи
                    chunk_counter += 1
                    chunks.append(DocumentChunk(
                        document_id=document_id,
                        chunk_number=chunk_counter,
                        text=article_text,
                        metadata={
                            'type': 'article',
                            'article_number': article_number,
                            'items': items,
                            'keywords': self._extract_keywords(article_text),
                            'doc_title': doc_metadata.get('title', ''),
                            'doc_date': doc_metadata.get('adoption_date', ''),
                            'current_section': current_section
                        }
                    ))
                    logger.debug(f"Created article chunk: {article_number}")
            except Exception as e:
                logger.warning(f"Error processing article: {e}", exc_info=True)
                continue
                
        if not chunks and text:
            chunks.append(DocumentChunk(
                document_id=document_id,
                chunk_number=1,
                text=text[:1000],
                metadata=doc_metadata
            ))
            logger.warning("Could not extract structured content, created generic chunk")
            
        return chunks

    def _extract_document_metadata(self, text: str) -> dict:
        """Извлечение общих метаданных документа"""
        metadata = {}
        
        title_match = re.search(
            r'"([^"]+)"\s+от\s+(\d{2}\.\d{2}\.\d{4})',
            text
        )
        if title_match:
            metadata['title'] = title_match.group(1).strip()
            try:
                metadata['adoption_date'] = datetime.strptime(
                    title_match.group(2), 
                    '%d.%m.%Y'
                ).date()
            except:
                pass
                
        # Определение типа документа
        if re.search(r'ГРАЖДАНСКИЙ\s+КОДЕКС|Гражданский\s+кодекс', text):
            metadata['doc_type'] = 'codex'
        elif re.search(r'ФЕДЕРАЛЬНЫЙ\s+ЗАКОН|Федеральный\s+закон', text):
            metadata['doc_type'] = 'federal_law'
        else:
            #TODO: Добавить другие типы документов
            pass
        
        doc_number_match = re.search(
            r'[NН]\s+(\d+(?:-\w+)?)', 
            text
        )
        if doc_number_match:
            metadata['document_number'] = doc_number_match.group(1)
            
        edition_match = re.search(
            r'ред\.\s+от\s+(\d{2}\.\d{2}\.\d{4})', 
            text
        )
        if edition_match:
            try:
                metadata['last_edition'] = datetime.strptime(
                    edition_match.group(1), 
                    '%d.%m.%Y'
                ).date()
            except:
                pass
                
        return metadata

    def _extract_keywords(self, text: str) -> list[str]:
        """Извлечение ключевых терминов из текста"""
        terms = [
            r'обязательство', r'договор', r'право', r'ответственность', r'сделка', 
            r'иск', r'возмещение', r'ущерб', r'закон', r'кодекс', r'ст\.', r'п\.',
            r'собственность', r'имущество', r'наследство', r'наследование',
            r'обязательств', r'защита', r'компенсация', r'регулирование', r'владение',
            r'правоотношения', r'субъект', r'объект', r'правонарушение',
            r'дееспособность', r'правоспособность', r'представительство',
            r'исковая давность', r'сервитут', r'залог'
        ]
        pattern = '|'.join(fr'{term}' for term in terms)
        keywords = re.findall(pattern, text, flags=re.IGNORECASE)
        return list(set(keywords)) 