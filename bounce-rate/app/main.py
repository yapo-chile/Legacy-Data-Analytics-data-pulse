# pylint: disable=no-member
# utf-8
import sys
import logging
from infraestructure.athena import Athena
from infraestructure.conf import getConf
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from utils.time_execution import TimeExecution


if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('pulse-bounce-rate')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    ATHENA = Athena(conf=CONFIG.athenaConf)
    QUERY = Query()
    DATA = ATHENA.get_data(QUERY.query_bounce_rate(PARAMS))
    ATHENA.close_connection()
    DB_WRITE = Database(conf=CONFIG.db)
    DB_WRITE.execute_command(QUERY.delete_bounce_rate(PARAMS))
    DB_WRITE.insert_data(DATA)
    DB_WRITE.close_connection()
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
