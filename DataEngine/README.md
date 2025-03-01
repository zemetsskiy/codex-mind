## Установка и запуск

#### Предварительные требования

- Python 3.9+
- Docker

### Шаг 1: Установка и запуск Qdrant
```
docker pull qdrant/qdrant

mkdir -p ./qdrant_storage

docker run -d -p 6333:6333 -p 6334:6334 \
    -v $(pwd)/qdrant_storage:/qdrant/storage \
    --name qdrant qdrant/qdrant
```

### Шаг 2: Проверка подключения к Qdrant

```
curl http://localhost:6333/livez
```

### Шаг 3: Настройка виртуального окружения

```
python -m venv venv

source venv/bin/activate

pip install -r requirements.txt
```

### Шаг 4: Подготовка данных

```
mkdir -p data/documents

# Копирование документов в директорию (если у вас есть исходные файлы)
cp /path/to/your/documents/*.txt data/documents/
```

### Шаг 5: Индексация документов

#### Индексация одного документа
```
python main.py single --input-file data/documents/document1.txt
```
#### Дополнительные опции
 
- Пакетная обработка всех документов в директории
```
python main.py batch --input-dir data/documents
```

- Для полной переиндексации (удаляет существующие данные)
```
python main.py reindex --input-dir data/documents --confirm
```

### Шаг 6: Проверка данных в системе

#### Поиск по коллекции документов

```
python tests/search_documents.py search "ответственность за неисполнение договора"
```

#### Анализ данных в Qdrant

```
python tests/inspect_qdrant.py info
```