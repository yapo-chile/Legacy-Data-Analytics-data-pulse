# pylint: disable=no-member
# utf-8
import sys
import logging
import environ
from infraestructure.athena import Athena
from infraestructure.conf import AppConfig
from infraestructure.psql import Database
from interfaces.query import Query
from interfaces.read_params import ReadParams
from interfaces.time_execution import TimeExecution


if __name__ == '__main__':
    CONFIG = environ.to_config(AppConfig)
    TIME = TimeExecution()
    LOGGER = logging.getLogger('pulse-bounce-rate')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    ATHENA_CONNECT = Athena(CONFIG.athena.s3_bucket,
                            CONFIG.athena.user,
                            CONFIG.athena.access_key,
                            CONFIG.athena.secret_key,
                            CONFIG.athena.region)
    QUERY = Query()
    DATA = ATHENA_CONNECT.get_data(QUERY.query_events(PARAMS.get_date_from(),
                                                      PARAMS.get_date_to()))
    DB_WRITE = Database(CONFIG.db.host,
                        CONFIG.db.port,
                        CONFIG.db.name,
                        CONFIG.db.user,
                        CONFIG.db.password)
    DB_WRITE.execute_command(QUERY.delete_events(PARAMS.get_date_from(),
                                                 PARAMS.get_date_to()))
    DB_WRITE.insert_events(CONFIG.db.table,
                           DATA)
    ATHENA_CONNECT.close_connection()
    DB_WRITE.close_connection()
    TIME.get_time()
    LOGGER.info('Process ended successed.')
