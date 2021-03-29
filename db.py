import os
from sqlalchemy import create_engine
from log import get_logger

logger = get_logger(__name__)


def db_engine():
    try:
        logger.info('Connect to DB')
        host = os.environ.get('DB_HOST', 'localhost')
        port = os.environ.get('DB_PORT', '3306')
        name = os.environ.get('DB_NAME', 'python_test')
        user = os.environ.get('DB_USER', 'root')
        pwd = os.environ.get('DB_PASS', '')

        driver = 'mysql+pymysql'

        engine = create_engine(f'{driver}://{user}:{pwd}@{host}/{name}?charset=utf8mb4')

        return engine.connect()
    except Exception as e:
        logger.error(e)