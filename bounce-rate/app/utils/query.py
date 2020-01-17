from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def query_bounce_rate(self, params: ReadParams) -> str:
        year: str = str(int(params.get_current_year()))
        month: str = str(int(params.get_current_month()))
        day: str = str(int(params.get_current_day()))
        query = """
                select 
                    timedate,
                    platform,
                    traffic_channel,
                    count(distinct case when events = 1 then session_id end)
                        bouncing_visits,
                    count(distinct session_id) visits
                from 
                (
                select
                    case
                    when length(cast(month as varchar)) = 1
                        and length(cast(day as varchar)) = 1
                    then (cast(year as varchar) 
                            || '-0' || cast(month as varchar) 
                            || '-0' || cast(day as varchar))
                    when length(cast(month as varchar)) = 1 
                        and length(cast(day as varchar)) = 2
                    then (cast(year as varchar)
                            || '-0' || cast(month as varchar)
                            || '-' || cast(day as varchar))
                    when length(cast(month as varchar)) = 2
                        and length(cast(day as varchar)) = 1
                    then (cast(year as varchar)
                            || '-' || cast(month as varchar)
                            || '-0' || cast(day as varchar))
                    when length(cast(month as varchar)) = 2 
                        and length(cast(day as varchar)) = 2
                    then (cast(year as varchar)
                        || '-' || cast(month as varchar)
                        || '-' || cast(day as varchar))
                    end timedate,
                    'All Yapo' platform,
                    case when traffic_channel = 'Direct' then 'Direct'
                         when traffic_channel = 'SEO' then 'SEO'
                         when traffic_channel in ('Social Media',
                                                  'Social Media Paid',
                                                  'Paid search','Display')
                                                  then 'Sponsored Links'
                         when traffic_channel in ('Affiliates',
                                                  'Email marketing',
                                                  'unknown') then 'Others'
                    end traffic_channel,   
                    session_id,
                    count(distinct event_id) events
                from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
                where
                    year = {0} and month = {1} and day = {2}
                and object_type in 
                    ('Listing',
                     'ClassifiedAd',
                     'Page',
                     'Error',
                     'Confirmation',
                     'Frontpage',
                     'Form',
                     'Content') 
                and event_type in ('View','Create')
                group by 1,2,3,4
                union all
                select
                    case 
                    when length(cast(month as varchar)) = 1
                        and length(cast(day as varchar)) = 1
                    then (cast(year as varchar) || '-0' ||
                        cast(month as varchar) || '-0' || cast(day as varchar))
                    when length(cast(month as varchar)) = 1
                        and length(cast(day as varchar)) = 2
                    then (cast(year as varchar) || '-0' ||
                        cast(month as varchar) || '-' || cast(day as varchar))
                    when length(cast(month as varchar)) = 2
                        and length(cast(day as varchar)) = 1
                    then (cast(year as varchar) || '-' ||
                        cast(month as varchar) || '-0' || cast(day as varchar))
                    when length(cast(month as varchar)) = 2
                        and length(cast(day as varchar)) = 2
                    then (cast(year as varchar) || '-' ||
                        cast(month as varchar) || '-' || cast(day as varchar))
                    end timedate,
                'All Yapo' platform,
                'All Yapo' traffic_channel,   
                session_id,
                count(distinct event_id) events
                from
                    yapocl_databox.insights_events_behavioral_fact_layer_365d
                where
                    year = {0} and month = {1} and day = {2}
                and object_type in ('Listing',
                                    'ClassifiedAd',
                                    'Page',
                                    'Error',
                                    'Confirmation',
                                    'Frontpage',
                                    'Form',
                                    'Content') 
                and event_type in ('View','Create')
                group by 1,2,3,4
                union all
                select
                    case 
                    when length(cast(month as varchar)) = 1
                        and length(cast(day as varchar)) = 1
                    then (cast(year as varchar) || '-0' ||
                        cast(month as varchar) || '-0' || cast(day as varchar))
                    when length(cast(month as varchar)) = 1
                        and length(cast(day as varchar)) = 2
                    then (cast(year as varchar) || '-0' ||
                        cast(month as varchar) || '-' || cast(day as varchar))
                    when length(cast(month as varchar)) = 2
                        and length(cast(day as varchar)) = 1
                    then (cast(year as varchar) || '-' ||
                        cast(month as varchar) || '-0' || cast(day as varchar))
                    when length(cast(month as varchar)) = 2
                        and length(cast(day as varchar)) = 2
                    then (cast(year as varchar) || '-' ||
                        cast(month as varchar) || '-' || cast(day as varchar))
                    end timedate,
                    case 
                        when product_type in ('AndroidApp','AndroidTabletApp')
                            then 'AndroidApp'
                        when product_type in ('iOSApp','iPadApp')
                            then 'iOSApp'
                        when product_type = 'M-Site'
                            then 'MSite'
                        when product_type = 'Web'
                            then 'Web'
                        else 'Others'
                    end platform,
                    'All Yapo' traffic_channel,   
                    session_id,
                    count(distinct event_id) events
                from
                    yapocl_databox.insights_events_behavioral_fact_layer_365d
                where
                    year = {0} and month = {1} and day = {2}
                and object_type in ('Listing',
                                    'ClassifiedAd',
                                    'Page',
                                    'Error',
                                    'Confirmation',
                                    'Frontpage',
                                    'Form',
                                    'Content') 
                and event_type in ('View','Create')
                group by 1,2,3,4
                union all
                select
                    case
                    when length(cast(month as varchar)) = 1
                        and length(cast(day as varchar)) = 1
                    then (cast(year as varchar) || '-0' ||
                        cast(month as varchar) || '-0' || cast(day as varchar))
                    when length(cast(month as varchar)) = 1
                        and length(cast(day as varchar)) = 2
                    then (cast(year as varchar) || '-0' ||
                        cast(month as varchar) || '-' || cast(day as varchar))
                    when length(cast(month as varchar)) = 2
                        and length(cast(day as varchar)) = 1
                    then (cast(year as varchar) || '-' ||
                        cast(month as varchar) || '-0' || cast(day as varchar))
                    when length(cast(month as varchar)) = 2
                        and length(cast(day as varchar)) = 2
                    then (cast(year as varchar) || '-' ||
                        cast(month as varchar) || '-' || cast(day as varchar))
                    end timedate,
                    case 
                    when product_type in ('AndroidApp','AndroidTabletApp')
                        then 'AndroidApp'
                    when product_type in ('iOSApp','iPadApp')
                        then 'iOSApp'
                    when product_type = 'M-Site'
                        then 'MSite'
                    when product_type = 'Web'
                        then 'Web'
                    else 'Others'
                    end platform,
                    case 
                    when traffic_channel = 'Direct' 
                        then 'Direct'
                    when traffic_channel = 'SEO' 
                        then 'SEO'
                    when traffic_channel in ('Social Media',
                                             'Social Media Paid',
                                             'Paid search',
                                             'Display') 
                        then 'Sponsored Links'
                    when traffic_channel in ('Affiliates',
                                             'Email marketing',
                                             'unknown') 
                        then 'Others'
                    end traffic_channel,
                    session_id,
                    count(distinct event_id) events
                from
                    yapocl_databox.insights_events_behavioral_fact_layer_365d
                where
                    year = {0} and month = {1} and day = {2}
                and object_type in ('Listing',
                                    'ClassifiedAd',
                                    'Page',
                                    'Error',
                                    'Confirmation',
                                    'Frontpage',
                                    'Form',
                                    'Content')
                and event_type in ('View','Create')
                group by 1,2,3,4
                order by 1,2,3,4
                ) a
                group by 1,2,3
                order by 1,2,3
                """.format(year,
                           month,
                           day)
        return query

    def delete_bounce_rate(self, params: ReadParams) -> str:
        """
        Method that returns events of the day
        """
        command = """
                  delete from dm_pulse.bounce_rate
                  where timedate::date between
                  '""" + params.get_date_from() + """'::date
                  and '""" + params.get_date_to() + """'::date """
        return command
