import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(name)s | %(funcName)s | %(levelname)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
