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

# Query traffic metrics from Pulse bucket
def source_traffic_metrics(params: ReadParams,
                           config: getConf):
    athena = Athena(conf=CONFIG.athenaConf)
    query = Query(config, params)
    data_athena = athena.get_data(query.query_traffic_metrics())
    athena.close_connection()
    return data_athena

# Query unique leads from Pulse bucket
def source_unique_leads(params: ReadParams,
                        config: getConf):
    athena = Athena(conf=CONFIG.athenaConf)
    query = Query(config, params)
    data_athena = athena.get_data(query.query_unique_leads())
    athena.close_connection()
    return data_athena

# Query unique leads from Pulse bucket
def source_conversion_to_lead(params: ReadParams,
                              config: getConf):
    athena = Athena(conf=CONFIG.athenaConf)
    query = Query(config, params)
    data_athena = athena.get_data(query.query_conversion_to_lead())
    athena.close_connection()
    return data_athena

# Query buyers from data warehouse
def source_buyers(params: ReadParams,
                  config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.db)
    data_dwh = db_source.select_to_dict(query \
                                        .query_buyers())
    db_source.close_connection()
    return data_dwh

# Query data from data warehouse
def source_dau_platform(params: ReadParams,
                        config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.db)
    data_dwh = db_source.select_to_dict(query \
                                        .query_dau_platform())
    db_source.close_connection()
    return data_dwh

# Write data to data warehouse
def write_data_pulse_to_dwh(params: ReadParams,
                            config: getConf,
                            data_traffic_metrics: pd.DataFrame,
                            data_unique_leads: pd.DataFrame,
                            data_conversion_to_lead: pd.DataFrame) -> None:
    query = Query(config, params)
    DB_WRITE = Database(conf=config.db)
    DB_WRITE.execute_command(query.delete_traffic_metrics())
    DB_WRITE.execute_command(query.delete_unqiue_leads())
    DB_WRITE.execute_command(query.delete_conversion_to_lead())
    DB_WRITE.insert_data_traffic_metrics(data_traffic_metrics)
    DB_WRITE.insert_data_unique_leads(data_unique_leads)
    DB_WRITE.insert_data_conversion_to_lead(data_conversion_to_lead)
    DB_WRITE.close_connection()

# Write data from Pulse to dwh
def write_aggregations_peak(params: ReadParams,
                            config: getConf,
                            data_buyer: pd.DataFrame,
                            data_dau_platform: pd.DataFrame) -> None:
    query = Query(config, params)
    DB_WRITE = Database(conf=config.db)
    DB_WRITE.execute_command(query.delete_buyers())
    DB_WRITE.execute_command(query.delete_dau_platform())
    DB_WRITE.insert_data_buyer(data_buyer)
    DB_WRITE.insert_data_dau_platform(data_dau_platform)
    DB_WRITE.close_connection()

if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('traffic-metrics')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    # Get data from Pulse and write in our dwh
    DATA_TRAFFIC_METRICS = source_traffic_metrics(PARAMS, CONFIG)
    DATA_UNIQUE_LEADS = source_unique_leads(PARAMS, CONFIG)
    DATA_CONVERSION_TO_LEAD = source_conversion_to_lead(PARAMS, CONFIG)
    write_data_pulse_to_dwh(PARAMS,
                            CONFIG,
                            DATA_TRAFFIC_METRICS,
                            DATA_UNIQUE_LEADS,
                            DATA_CONVERSION_TO_LEAD)
    # Get data for aggregations in our data warehouse
    DATA_BUYERS = source_buyers(PARAMS, CONFIG)
    DATA_DAU_PLATFORM = source_dau_platform(PARAMS, CONFIG)
    write_aggregations_peak(PARAMS, CONFIG, DATA_BUYERS, DATA_DAU_PLATFORM)
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
