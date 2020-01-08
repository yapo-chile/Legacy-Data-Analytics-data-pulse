
class Query:
    """
    Class that store all querys
    """
    def query_events(self, date_from, date_to):

        query = """
                select event_type,
                   object_type,
                   event_name,
                   event_date,
                   sum(events) as amount_events
                from yapocl_databox.insights_events_behavioral_counts_365d 
                where event_date between '""" + date_from + """'
                and '""" + date_to + """'
                group by 1,2,3,4
                order by 1,2,3,4 desc ;
                """
        return query

    def delete_events(self, date_from, date_to):
        """
        Method that returns events of the day
        """
        command = """
                  delete from dm_pulse.monitoring_events
                  where event_date::date between
                  '""" + date_from + """'::date
                  and '""" + date_to + """'::date """
        return command
