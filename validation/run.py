"""
Module to enter parameters and run validation module.
"""
import logging
import logging.config
from datetime import datetime
from sys import argv
from time import time
from quality_report import QualityReport

# Logging
today = datetime.today().strftime('%Y%m%d')
logging.config.dictConfig(
    {
        'disable_existing_loggers':True,
        'version':1
    }
)
logging.basicConfig(
    filename='logfile',
    encoding='utf-8',
    format='%(asctime)s %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S',
    level=logging.INFO
 )
logging.info('Logging started.')
logger = logging.getLogger()


# Parameters
data_filepath = argv[1]
metadata_filepath = argv[2]
vendor_name = argv[3]
bucket_name = argv[4]

# Run validation module
logger.info('Parameters entered, starting validation.')
start = time()
qr = QualityReport(data_filepath, metadata_filepath, vendor_name, bucket_name)
end = time()
total_time = end - start

logger.info('Finished validation. Time required : %s sec', total_time)
