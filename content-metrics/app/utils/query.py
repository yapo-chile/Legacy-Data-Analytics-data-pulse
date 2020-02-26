from infraestructure.conf import getConf
from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def query_nia(self) -> str:
        """
        Method return str with query
        """
        query = """
        select 
                a.creation_date::Date date_id, 
                count(distinct case when pl.platform_name = 'M Site'
                then ad_id_pk end) "nia_cg_M Site",
                count(distinct case when pl.platform_name = 'NGA Ios'
                then ad_id_pk end) "nia_cg_NGA Ios"
            from 
                ods.ad a 
            left join
                ods.platform pl on
                pl.platform_id_pk=a.platform_id_fk
            where
                category_id_fk in (28,
                                   29,
                                   30,
                                   31,
                                   37,
                                   38,
                                   39,
                                   50,
                                   40,
                                   41,
                                   42,
                                   43,
                                   19,
                                   20,
                                   21,
                                   22,
                                   23,
                                   24,
                                   25,
                                   26,
                                   36,
                                   44,
                                   45,
                                   46) 
                and creation_date::date = '{}'
                and pl.platform_name in ('M Site','NGA Ios')
            group by 1
            order by 1
        """.format(self.params.get_date_from())
        return query

    def query_nia_msite(self) -> str:
        """
        Method return str with query
        """
        query = """
        select 
            a.creation_date::Date date_id,
            count(distinct ad_id_pk) nia_msite
        from 
            ods.ad a 
        left join 
            ods.platform pl on pl.platform_id_pk=a.platform_id_fk
        where
            creation_date::date = '{}'
            and pl.platform_name in ('M Site')
        group by 1
        order by 1
        """.format(self.params.get_date_from())
        return query

    def query_naa(self) -> str:
        """
        Method return str with query
        """
        query = """
        select 
                a.approval_date::Date date_id,
                count(distinct case when pl.platform_name = 'NGA Android' then ad_id_pk end) "naa_cg_NGA Android",
                count(distinct case when pl.platform_name = 'Web' then ad_id_pk end) "naa_cg_Web",
                count(distinct a.seller_id_fk) sellers_cg
            from 
                ods.ad a 
            left join 
                ods.platform pl on pl.platform_id_pk=a.platform_id_fk
            where
                category_id_fk in (28,29,30,31,37,38,39,50,40,41,42,43,19,20,21,22,23,24,25,26,36,44,45,46) 
                --and pl.platform_name in ('NGA Android', 'Web')
                and approval_date::date = '{}'
            group by 1
            order by 1
        """.format(self.params.get_date_from())
        return query

    def query_ad_insert_visits(self) -> str:
        """
        Method return str with query
        """
        query = """
            select 
                cast(date_parse(cast(year as varchar) || '-' ||
                cast(month as varchar) || '-' || cast(day as varchar),
                '%Y-%c-%e') as date) date_id,
                cast( count(*) as integer) msite_adInsert_views
            from 
                yapocl_databox.insights_events_behavioral_fact_layer_365d x
            where
                object_url like '%publica-un-aviso%'
                and event_type='View' and object_type <> 'UIElement'
                and product_type = 'M-Site'
                and cast(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) = date '{0}'
                group by 1
                order by 1
        """.format(self.params.get_date_from())
        return query

    def query_ads_with_leads(self) -> str:
        """
        Method return str with query
        """
        query = """
        select
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),
            '%Y-%c-%e') as date)  + interval '7' day date_id, 
            sum(case when metric_name = 'naa_with_leads_7d'
            then metric_value end) / sum(case when metric_name = 'naa'
            then metric_value end) naa_with_leads_7d
        from
            yapocl_databox.insights_core_business_365d
        where
            client = 'yapocl'
            and device_type = 'ALL'
            and product_type = 'ALL'
            and tracker_type = 'ALL'
            and local_category_level2 = 'ALL'
            and local_category_level1 = 'ALL'
            and local_main_category = 'ALL'
            and local_vertical = 'ALL'
            and local_ad_lister_type = 'ALL'
            and local_ad_region_level2 = 'ALL'
            and local_ad_region_level1 = 'ALL'
            and traffic_channel = 'ALL'
            and traffic_source = 'ALL'
            and metric_name in  ('naa','naa_with_leads_7d')
            and cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),
            '%Y-%c-%e') as date)  + interval '7' day =  date '{0}'
        group by 1
        order by 1
        """.format(self.params.get_date_from())
        return query

    def delete_data(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from """ + self.conf.db.table + """ where
                    date_id::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command
