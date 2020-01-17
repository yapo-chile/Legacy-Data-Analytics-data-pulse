# pylint: disable=no-member
# utf-8
import sys
import logging
import pandas as pd
from infraestructure.athena import Athena
from infraestructure.conf import getConf
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from utils.time_execution import TimeExecution

# Query data from Pulse bucket
def source_data_pulse(params: ReadParams,
                      config: getConf):
    athena = Athena(conf=CONFIG.athenaConf)
    query = Query()
    data_athena = athena.get_data(query.query_bounce_rate(PARAMS))
    athena.close_connection()
    return data_athena

# Write data to data warehouse
def write_data_dwh(params: ReadParams,
                   config: getConf,
                   data_athena: pd.DataFrame) -> None:
    query = Query()
    DB_WRITE = Database(conf=config.db)
    DB_WRITE.execute_command(query.delete_bounce_rate(PARAMS))
    DB_WRITE.insert_data(data_athena)
    DB_WRITE.close_connection()


if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('pulse-bounce-rate')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    DATA_ATHENA = source_data_pulse(PARAMS, CONFIG)
    write_data_dwh(PARAMS, CONFIG, DATA_ATHENA)
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
