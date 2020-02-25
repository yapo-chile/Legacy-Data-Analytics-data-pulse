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
def source_pulse_ads_metrics(params: ReadParams,
                             config: getConf):
    athena = Athena(conf=CONFIG.athenaConf)
    query = Query(config, params)
    data_ad_insert_visits = athena.get_data(query.query_ad_insert_visits())
    data_ads_with_leads = athena.get_data(query.query_ads_with_leads())
    athena.close_connection()
    return data_ad_insert_visits, data_ads_with_leads

# Query data from data warehouse
def source_dwh_nia(params: ReadParams,
                   config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.db)
    data_nia = db_source.select_to_dict(query \
                                        .query_nia())
    data_nia_msite = db_source.select_to_dict(query \
                                        .query_nia_msite())
    data_naa = db_source.select_to_dict(query \
                                        .query_naa())
    db_source.close_connection()
    return data_nia, data_nia_msite, data_naa

# Write data to data warehouse
def write_data_dwh(params: ReadParams,
                   config: getConf,
                   data: pd.DataFrame) -> None:
    query = Query(config, params)
    DB_WRITE = Database(conf=config.db)
    DB_WRITE.execute_command(query.delete_data())
    DB_WRITE.insert_data(data)
    DB_WRITE.close_connection()

if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('content-metrics')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    DF_AD_INSERT, DF_AD_LEADS = source_pulse_ads_metrics(PARAMS, CONFIG)
    DF_NIA, DF_NIA_MSITE, DF_NAA = source_dwh_nia(PARAMS, CONFIG)

    DF_NAA.set_index('date_id', inplace=True)
    DF_NIA_MSITE.set_index('date_id', inplace=True)
    DF_AD_INSERT.set_index('date_id', inplace=True)
    DF_AD_LEADS.set_index('date_id', inplace=True)

    DF_MERGE = DF_NAA.merge(DF_NIA, on="date_id", how='left')
    DF_MERGE = DF_MERGE.merge(DF_NIA_MSITE, on="date_id", how='left')
    DF_MERGE = DF_MERGE.merge(DF_AD_INSERT, on="date_id", how='left')
    DF_MERGE = DF_MERGE.merge(DF_AD_LEADS, on="date_id", how='left')
    DF_MERGE = DF_MERGE. \
        rename(columns={'naa_cg_NGA Android': 'naa_cg_NGA_Android'})
    DF_MERGE = DF_MERGE.rename(columns={'nia_cg_M Site': 'nia_cg_M_Site'})
    DF_MERGE = DF_MERGE.rename(columns={'nia_cg_NGA Ios': 'nia_cg_NGA_Ios'})
    write_data_dwh(PARAMS, CONFIG, DF_MERGE)
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
