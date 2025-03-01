import logging
from pathlib import Path
import sys

# Инициализируем корневой логгер
logger = logging.getLogger(__name__)

def setup_logging():
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(logs_dir / 'legal_rag.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logger 