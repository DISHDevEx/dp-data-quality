"""
Module to enter parameters and run validation module.
"""
import logging
import logging.config
import sys
from time import time
from validation import QualityReport

# Logging
sys.tracebacklimit = 0
logging.config.dictConfig(
    {
        'disable_existing_loggers':True,
        'version':1
    }
)
logging.basicConfig(
    filename='./validation/logfile.log',
    encoding='utf-8',
    format='%(asctime)s %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S %p %Z',
    level=logging.INFO
 )
logging.info('Logging started.')
logger = logging.getLogger()


# Parameters
data_filepath = sys.argv[1]
metadata_filepath = sys.argv[2]
vendor_name = sys.argv[3]
bucket_name = sys.argv[4]

# Run validation module
logger.info('Parameters entered, starting validation.')
start = time()
qr = QualityReport(data_filepath, metadata_filepath, vendor_name, bucket_name)
end = time()
total_time = end - start

logger.info('Finished validation. Time required : %s sec', total_time)
