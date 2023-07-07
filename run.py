"""
Module to enter parameters and run validation module.
"""
import logging
import logging.config
import sys
from time import time
from dp_data_quality import QualityReport

# Logging
# sys.tracebacklimit = 0
logging.config.dictConfig(
    {
        'disable_existing_loggers':True,
        'version':1
    }
)
logging.basicConfig(
    filename='./dp_data_qualit/logfile.log',
    encoding='utf-8',
    format='%(asctime)s %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S %p %Z',
    level=logging.INFO
 )
logger = logging.getLogger()


# Parameters
data_filepath = sys.argv[1]
metadata_filepath = sys.argv[2]
account_id = sys.argv[3]
bucket_name = sys.argv[4]

# Run validation module
logger.info('---------------------------------------------------')
logger.info('Running data quality checks on %s data', account_id)
start = time()
qr = QualityReport(data_filepath, metadata_filepath, account_id, bucket_name)
end = time()
total_time = end - start
logger.info('Ran data quality checks on %s table', qr.table_name)
if qr.report_url.endswith('txt'):
    logger.info('No data quality issues were discovered in data')
logger.info('Quality report saved at: %s', qr.report_url)
logger.info('Time required : %s sec', total_time)
logger.info('--------------------------------------------------')
