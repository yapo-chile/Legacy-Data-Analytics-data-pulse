
class Query:
    """
    Class that store all querys
    """
    def query_events(self, date_from, date_to):

        query = """
            SELECT
                a.event_type,                    
                a.object_type,
                a.event_name,
                a.event_date,
                count(*) amount_events
            FROM (  
                select 
                    event_type,                    
                    object_type,
                    event_name,
                    cast(date_parse(cast(year as varchar) || '-' ||
                    cast(month as varchar) || '-' || cast(day as varchar),
                    '%Y-%c-%e') as date) event_date
                from yapocl_databox.insights_events_behavioral_fact_layer_365d
                where cast(date_parse(cast(year as varchar) || '-' ||
                    cast(month as varchar) || '-' || cast(day as varchar),
                    '%Y-%c-%e') as date) = date '""" + date_from + """'
                union all
                select 
                    event_type,                    
                    object_type,
                    event_name,
                    cast(date_parse(cast(year as varchar) || '-' ||
                    cast(month as varchar) || '-' || cast(day as varchar),
                    '%Y-%c-%e') as date) event_date
                from yapocl_databox.insights_events_content_fact_layer_365d
                where cast(date_parse(cast(year as varchar) || '-' ||
                    cast(month as varchar) || '-' || cast(day as varchar),
                    '%Y-%c-%e') as date) = date '""" + date_from + """'
            ) a
            GROUP BY 1,2,3,4
            ORDER BY 1,2,3,4
        """
        return query

    def delete_events(self, date_from, date_to):
        """
        Method that returns events of the day
        """
        command = """
                  delete from dm_pulse.monitoring_events
                  where event_date::date =
                  '""" + date_from + """'::date
                  """
        return command
