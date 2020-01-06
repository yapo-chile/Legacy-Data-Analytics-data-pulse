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
    DATA = ATHENA.get_data(QUERY.query_events(PARAMS.get_date_from(),
                                              PARAMS.get_date_to()))
    DB_WRITE = Database(conf=CONFIG.db)
    DB_WRITE.execute_command(QUERY.delete_events(PARAMS.get_date_from(),
                                                 PARAMS.get_date_to()))
    DB_WRITE.insert_events(CONFIG.db.table,
                           DATA)
    ATHENA.close_connection()
    DB_WRITE.close_connection()
    TIME.get_time()
    LOGGER.info('Process ended successed.')
