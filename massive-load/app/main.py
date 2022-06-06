# pylint: disable=no-member
# utf-8
import sys
import logging
import pickle
from datetime import datetime, date
import pandas as pd
from infraestructure.athena import Athena
from infraestructure.conf import getConf
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams
from utils.time_execution import TimeExecution

# chunk function for query execution by part
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# Query data from Pulse bucket
def source_data_pulse(params: ReadParams,
                      config: getConf):
    athena = Athena(conf=config.athenaConf)
    query = Query(config, params)
    data_athena = athena.get_data(query.query_base_pulse())
    athena.close_connection()
    return data_athena

# Query data from data warehouse
def source_data_dwh(params: ReadParams,
                    config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.DWConf)
    data_dwh = db_source.select_to_dict(query.query_base_postgresql_dw())
    db_source.close_connection()
    return data_dwh

# Query data from blocket DB
def source_data_blocket(params: ReadParams,
                        config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.blocketConf)
    data_blocket = db_source.select_to_dict( \
        query.query_base_postgresql_blocket())
    db_source.close_connection()
    return data_blocket

# Query data from Pulse bucket
def source_data_gbq_partners_leads(params: ReadParams,
                                   config: getConf,
                                   listIds):
    query = Query(config, params)
    LOGGER.info('QUERY GBQ PARTNERS LEAD: ' + str(query.get_gbq_partners_leads(listIds)))
    data = pd.read_gbq(query.get_gbq_partners_leads(listIds), project_id='yapo-dat-prd')
    LOGGER.info('DATA GBQ PARTERNS LEAD:')
    LOGGER.info(data)
    return data

# Query data from blocket DB
def source_data_blocket_partner_ads(params: ReadParams,
                                    config: getConf):
    query = Query(config, params)
    db_source = Database(conf=config.blocketConf)
    data_blocket = db_source.select_to_dict(query.get_partner_ads())
    db_source.close_connection()
    return data_blocket

# Query data from blocket DB
def source_data_blocket_partner_ads_params(params: ReadParams,
                                           config: getConf,
                                           adIds):
    query = Query(config, params)
    db_source = Database(conf=config.blocketConf)
    data_blocket = pd.DataFrame()
    adIdsChunks = list(chunks(adIds, 10000))
    for chunk in adIdsChunks:
        df = db_source.select_to_dict(query.get_partner_ads_params(chunk))
        data_blocket = data_blocket.append(df, ignore_index=True)
    db_source.close_connection()
    return data_blocket

# Query data from blocket DB
def source_data_blocket_partner_ads_info(params: ReadParams,
                                         config: getConf,
                                         adIds):
    query = Query(config, params)
    db_source = Database(conf=config.blocketConf)
    data_blocket = pd.DataFrame()
    adIdsChunks = list(chunks(adIds, 10000))
    for chunk in adIdsChunks:
        df = db_source.select_to_dict(query.get_partner_ad_info(chunk))
        data_blocket = data_blocket.append(df, ignore_index=True)
    db_source.close_connection()
    return data_blocket

# Query data from blocket DB
def source_data_blocket_partner_users(params: ReadParams,
                                      config: getConf,
                                      userIds):
    query = Query(config, params)
    db_source = Database(conf=config.blocketConf)
    data_blocket = pd.DataFrame()
    userIdsChunks = list(chunks(userIds, 10000))
    for chunk in userIdsChunks:
        df = db_source.select_to_dict(query.get_partner_users(chunk))
        data_blocket = data_blocket.append(df, ignore_index=True)
    db_source.close_connection()
    return data_blocket

# Query data from blocket DB
def source_data_blocket_inmo_params(params: ReadParams,
                                    config: getConf,
                                    adIds):
    query = Query(config, params)
    db_source = Database(conf=config.blocketConf)
    data_blocket = pd.DataFrame()
    adIdsChunks = list(chunks(adIds, 10000))
    for chunk in adIdsChunks:
        df = db_source.select_to_dict(query.ad_inmo_params(chunk))
        data_blocket = data_blocket.append(df, ignore_index=True)
    db_source.close_connection()
    return data_blocket

# Query data from blocket DB
def source_data_blocket_car_params(params: ReadParams,
                                   config: getConf,
                                   adIds):
    query = Query(config, params)
    db_source = Database(conf=config.blocketConf)
    data_blocket = pd.DataFrame()
    adIdsChunks = list(chunks(adIds, 10000))
    for chunk in adIdsChunks:
        df = db_source.select_to_dict(query.ad_car_params(chunk))
        data_blocket = data_blocket.append(df, ignore_index=True)
    db_source.close_connection()
    return data_blocket

# Query data from pickles files
def source_data_pickles(config: getConf):
    pickles_obj = config.PicklesConf
    return pickles_obj

# Write data to data warehouse
def write_data_dwh(params: ReadParams,
                   config: getConf,
                   data_dwh: pd.DataFrame) -> None:
    query = Query(config, params)
    DB_WRITE = Database(conf=config.DWConf)
    DB_WRITE.execute_command(query.delete_base())
    DB_WRITE.insert_data(data_dwh)
    DB_WRITE.close_connection()

if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('massive-load')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)

 ###################################################
 #                     EXTRACT                     #
 ###################################################

    LOGGER.info("HORA INICIO:")
    LOGGER.info(datetime.now().strftime('%H:%M:%S'))

    LOGGER.info("Started extraction phase for: DATA_PARTNERS_ADS")
    DATA_PARTNERS_ADS = source_data_blocket_partner_ads(PARAMS, CONFIG)
    LOGGER.info(DATA_PARTNERS_ADS.head(20))

    DATA_PARTNERS_ADS['ad_id'] = DATA_PARTNERS_ADS['ad_id'].astype(int)
    DATA_PARTNERS_ADS['list_id'] = DATA_PARTNERS_ADS['list_id'].fillna(0)\
        .astype(int)
    DATA_PARTNERS_ADS_ID = DATA_PARTNERS_ADS['ad_id'].values
    DATA_PARTNERS_ADS_LIST_ID = DATA_PARTNERS_ADS['list_id'].values

    LOGGER.info("Started extraction phase for: DATA_PARTNERS_ADS_PARAMS")
    DATA_PARTNERS_ADS_PARAMS =\
        source_data_blocket_partner_ads_params(PARAMS,
                                               CONFIG,
                                               DATA_PARTNERS_ADS_ID)

    LOGGER.info("Started extraction phase for: DATA_PARTNERS_ADS_INFO")
    DATA_PARTNERS_ADS_INFO =\
        source_data_blocket_partner_ads_info(PARAMS,
                                             CONFIG,
                                             DATA_PARTNERS_ADS_ID)

    DATA_PARTNERS_ADS_INFO['user_id'] = DATA_PARTNERS_ADS_INFO['user_id']\
        .astype(int)
    DATA_PARTNERS_ADS_INFO_USER_IDS = DATA_PARTNERS_ADS_INFO['user_id']\
        .values

    LOGGER.info("Started extraction phase for: DATA_PARTNER_USERS")
    DATA_PARTNER_USERS =\
        source_data_blocket_partner_users(PARAMS,
                                          CONFIG,
                                          DATA_PARTNERS_ADS_INFO_USER_IDS)

    LOGGER.info("Started extraction phase for: DATA_INMO_PARAMS")
    DATA_INMO_PARAMS = source_data_blocket_inmo_params(PARAMS,
                                                       CONFIG,
                                                       DATA_PARTNERS_ADS_ID)

    LOGGER.info("Started extraction phase for: DATA_CAR_PARAMS")
    DATA_CAR_PARAMS = source_data_blocket_car_params(PARAMS,
                                                     CONFIG,
                                                     DATA_PARTNERS_ADS_ID)

    LOGGER.info("Started extraction phase for: DATA_PARTNERS_LEADS")
    DATA_PARTNERS_LEADS =\
        source_data_gbq_partners_leads(PARAMS,
                                       CONFIG,
                                       DATA_PARTNERS_ADS_LIST_ID)

 ###################################################
 #                   TRANSFORM                     #
 ###################################################
 # Cleaning list_id field from DATA_PARTNERS_LEADS
    LOGGER.info("Started list_id field cleaning from DATA_PARTNERS_LEADS")
    DATA_PARTNERS_LEADS = DATA_PARTNERS_LEADS[DATA_PARTNERS_LEADS["list_id"]\
        .notnull()]
    DATA_PARTNERS_LEADS = DATA_PARTNERS_LEADS[DATA_PARTNERS_LEADS["list_id"]\
        .str.isnumeric()]
    DATA_PARTNERS_LEADS["list_id"] = DATA_PARTNERS_LEADS["list_id"].astype(int)

 # Setting index to dataframes
    LOGGER.info("Started index setting to dataframes")
    DATA_PARTNERS_ADS.set_index('ad_id', inplace=True)
    DATA_PARTNERS_ADS_PARAMS.set_index('ad_id', inplace=True)
    DATA_PARTNERS_ADS_INFO.set_index('ad_id', inplace=True)
    DATA_PARTNER_USERS.set_index('user_id', inplace=True)

 # Drop duplicates in user data df
    LOGGER.info("Started drop duplicates in user data df")
    DATA_PARTNER_USERS = DATA_PARTNER_USERS.drop_duplicates()

 # Merging dataframes extracted from Blocket DB
    LOGGER.info("Started dataframes merging extracted from Blocket DB")
    PARTNERS_ADS_WITH_PARAMS = pd.merge(DATA_PARTNERS_ADS,
                                        DATA_PARTNERS_ADS_PARAMS,
                                        how="inner", on="ad_id")

    PARTNERS_ADS_WITH_PARAMS_AND_INFO = pd.merge(
        PARTNERS_ADS_WITH_PARAMS,
        DATA_PARTNERS_ADS_INFO,
        how="inner", on="ad_id")

    PARTNERS_ADS_WITH_PARAMS_AND_INFO = PARTNERS_ADS_WITH_PARAMS_AND_INFO.\
        reset_index()

    PARTNERS_ADS_WITH_PARAMS_AND_INFO = PARTNERS_ADS_WITH_PARAMS_AND_INFO[[
        "list_id", "ad_id", "list_time", "deletion_date", "vertical",
        "category", "integrador", "codigo_inmo", "patente", "region",
        "comuna", "price", "sucursal", "user_id"]]

    PARTNERS_ADS_PARAMS = pd.merge(
        PARTNERS_ADS_WITH_PARAMS_AND_INFO,
        DATA_PARTNER_USERS,
        how="inner", on="user_id")

 # Adding param info to ads
    LOGGER.info("Started param info adding to ads")
    DATA_INMO_PARAMS[["rooms", "squared_meters", "estate_type"]] = \
    DATA_INMO_PARAMS[["rooms", "squared_meters", "estate_type"]]. \
        fillna(0).astype(int)

    DATA_CAR_PARAMS[["car_year", "brand", "model", "km"]] = \
    DATA_CAR_PARAMS[["car_year", "brand", "model", "km"]]. \
        fillna(0).astype(int)

    PARTNERS_WITH_BASIC_PARAMS = pd.merge(
        PARTNERS_ADS_PARAMS,
        DATA_INMO_PARAMS,
        how="left", on=["ad_id"])

    PARTNERS_WITH_ALL_PARAMS = pd.merge(
        PARTNERS_WITH_BASIC_PARAMS,
        DATA_CAR_PARAMS,
        how="left", on=["ad_id"])

    PARTNERS_WITH_ALL_PARAMS[["region", "comuna", "rooms",
                              "squared_meters", "estate_type",
                              "car_year", "brand", "model",
                              "km", "price"]] = \
    PARTNERS_WITH_ALL_PARAMS[["region", "comuna", "rooms",
                              "squared_meters", "estate_type",
                              "car_year", "brand",
                              "model", "km", "price"]].fillna(0)

    PARTNERS_WITH_ALL_PARAMS[["region", "comuna", "rooms",
                              "squared_meters", "estate_type",
                              "car_year", "brand", "model",
                              "km", "price"]] = \
    PARTNERS_WITH_ALL_PARAMS[["region", "comuna", "rooms",
                              "squared_meters", "estate_type",
                              "car_year", "brand", "model",
                              "km", "price"]].astype(int)

 # Mapping values for params
    LOGGER.info("Started values mapping for params")
    PATH_PICKLES = source_data_pickles(CONFIG)

    with open(PATH_PICKLES.pickles_path + "/region_map.pickle", 'rb') \
        as handle: region_map = pickle.load(handle)

    with open(PATH_PICKLES.pickles_path + "/communes_map.pickle", 'rb') \
        as handle: communes_map = pickle.load(handle)

    with open(PATH_PICKLES.pickles_path + "/category_map.pickle", 'rb') \
        as handle: category_map = pickle.load(handle)

    with open(PATH_PICKLES.pickles_path + "/estate_type_map.pickle", 'rb') \
        as handle: estate_type_map = pickle.load(handle)

    with open(PATH_PICKLES.pickles_path + "/brand_map.pickle", 'rb') \
        as handle: brand_map = pickle.load(handle)

    with open(PATH_PICKLES.pickles_path + "/model_map.pickle", 'rb') \
        as handle: model_map = pickle.load(handle)

    PARTNERS_WITH_ALL_PARAMS["index_tuple"] = PARTNERS_WITH_ALL_PARAMS\
    .apply((lambda x: str(x["brand"]) + ',' + str(x["model"])), axis=1)

    PARTNERS_WITH_ALL_PARAMS["region"] = PARTNERS_WITH_ALL_PARAMS[
        "region"].map(region_map)

    PARTNERS_WITH_ALL_PARAMS["comuna"] = PARTNERS_WITH_ALL_PARAMS[
        "comuna"].map(communes_map)

    PARTNERS_WITH_ALL_PARAMS["category"] = PARTNERS_WITH_ALL_PARAMS[
        "category"].map(category_map)

    PARTNERS_WITH_ALL_PARAMS["estate_type"] = PARTNERS_WITH_ALL_PARAMS[
        "estate_type"].map(estate_type_map)

    PARTNERS_WITH_ALL_PARAMS["model"] = PARTNERS_WITH_ALL_PARAMS[
        "index_tuple"].map(model_map)

    PARTNERS_WITH_ALL_PARAMS["brand"] = PARTNERS_WITH_ALL_PARAMS[
        "brand"].map(brand_map)

 # Merging ads with activity from Pulse
    LOGGER.info("Started ads with activity merging from Pulse")
    DATA_PARTNERS = pd.merge(
        PARTNERS_WITH_ALL_PARAMS,
        DATA_PARTNERS_LEADS,
        how="left", on=["list_id"])

    DATA_PARTNERS["deletion_date"] = DATA_PARTNERS["deletion_date"]. \
        fillna(date(2199, 12, 31)).astype(str)

 # Filtering for active ads in the period
    LOGGER.info("Started active ads filtering in the period")
    DATA_PARTNERS_ACTIVE = DATA_PARTNERS[DATA_PARTNERS["list_time"]. \
        astype(str) <= DATA_PARTNERS["timedate"].astype(str)]

    DATA_PARTNERS_ACTIVE = DATA_PARTNERS_ACTIVE[(
        DATA_PARTNERS_ACTIVE["deletion_date"].astype(str) >=
        DATA_PARTNERS_ACTIVE["timedate"].astype(str))]

    DATA_PARTNERS_ACTIVE = DATA_PARTNERS_ACTIVE[
        ["timedate", "vertical", "category", "integrador", "sucursal",
         "email", "codigo_inmo", "patente", "list_id", "list_time",
         "deletion_date", "region", "comuna", "estate_type", "squared_meters",
         "rooms", "car_year", "brand", "model", "km", "price", "currency",
         "number_of_views", "number_of_show_phone", "number_of_calls",
         "number_of_ad_replies"
         ]]

 ###################################################
 #                     LOAD                        #
 ###################################################
    LOGGER.info("Started load phase in dm_analysis.test_partners_leads")
    write_data_dwh(PARAMS, CONFIG, DATA_PARTNERS_ACTIVE)
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
