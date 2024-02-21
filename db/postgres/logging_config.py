import logging
from datetime import datetime, timezone


class UTCFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.utcfromtimestamp(record.created)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + ' UTC'

# Logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Custom UTC formatter
formatter = UTCFormatter('%(asctime)s - %(levelname)s - %(message)s')

# Terminal output
th = logging.StreamHandler()
th.setLevel(logging.DEBUG)
th.setFormatter(formatter)
logger.addHandler(th)

# File output
fh = logging.FileHandler('logs.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

# Test logging
logger.info("=== LOGGING STARTED ===")
