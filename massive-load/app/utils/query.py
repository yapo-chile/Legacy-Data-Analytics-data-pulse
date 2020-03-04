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

    def query_base_postgresql(self) -> str:
        """
        Method return str with query
        """
        query = """
                select cast((now() - interval '1 day')::date as varchar)
                    as timedate,
	            version()  as current_version;
            """
        return query

    def query_partners_leads(self) -> str:
        """
        Method return str with query
        """
        query =  """
        select
            cast(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) timedate,
            split_part(ad_id,':',4) list_id,
            count(distinct case when event_type = 'View' and object_type = 'ClassifiedAd' then event_id end) number_of_views,
            count(distinct case when event_type = 'Call' then event_id end) number_of_calls,
            count(distinct case when event_type = 'Show' then event_id end) number_of_show_phone,
            count(distinct case when event_type = 'Send' then environment_id end) number_of_ad_replies
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' || cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e') as date) = date '{0}'
            and ad_id != 'sdrn:yapocl:classified:' and ad_id != 'sdrn:yapocl:classified:0'
            and local_main_category in ('inmuebles','vehiculos','vehículos','veh韈ulos','veh�culos')
        group by 1,2
        order by 1,2
        """.format(self.params.get_date_from())
        return query

    def delete_base(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_analysis.db_version where 
                    timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command
