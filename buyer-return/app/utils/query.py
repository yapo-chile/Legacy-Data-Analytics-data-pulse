from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def query_base_postgresql(self, params: ReadParams) -> str:
        """
        Method return str with query
        """
        query = """
                select cast((now() - interval '1 day')::date as varchar)
                    as timedate,
	            version()  as current_version;
            """
        return query

    def query_buyer_return_over_current(self, params: ReadParams) -> str:
        """
        Method return str with query
        """
        query = """
        select 
            date '{0}' dt_metric,
            (date '{0}' - interval '31' day) first_period_start,
            (date '{0}' - interval '2' day) first_period_end,
            (date '{0}' - interval '1' day) second_period,
            count(distinct case when b.environment_id is not null
                then b.environment_id end) buyers_first_period,
            count(distinct case when a.environment_id is not null
                then a.environment_id end) buyers_second_period,
            count(distinct case when a.environment_id is not null
                and b.environment_id is not null then a.environment_id end)
                buyers_both_periods
        from
            (--second period
            select distinct 
                environment_id
            from 
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where 
                client_id = 'yapocl' and event_type in ('Call','SMS','Send','Show')
                and cast(date_parse(cast(year as varchar) || '-' ||
                    cast(month as varchar) || '-' ||
                    cast(day as varchar),'%Y-%c-%e') as date) =
                        (date '{0}' - interval '1' day)
                and local_main_category in ('computadores & electrónica',
                                            'computadore & electronica',
                                            'computadores & electr�nica',
                                            'computadores y electronica',
                                            'futura mamá',
                                            'futura mam�',
                                            'futura mama bebes y ninos',
                                            'futura mamá bebés y niños',
                                            'futura mamá, bebés y niños',
                                            'futura mamá,bebés y niños',
                                            'futura mam� beb�s y ni�os',
                                            'futura mam�,beb�s y ni�os',
                                            'hogar',
                                            'moda',
                                            'moda calzado belleza y salud',
                                            'moda, calzado, belleza y salud',
                                            'moda,calzado,belleza y salud',
                                            'otros',
                                            'otros productos',
                                            'other',
                                            'tiempo libre')
            ) a
        full join 
            (--first period
            select distinct 
                environment_id
            from 
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where 
                client_id = 'yapocl' and event_type in ('Call','SMS','Send','Show')
                and cast(date_parse(cast(year as varchar) || '-' ||
                    cast(month as varchar) || '-' ||
                    cast(day as varchar),'%Y-%c-%e') as date)
                between (date '{0}' - interval '31' day)
                    and (date '{0}' - interval '2' day)
                and local_main_category in ('computadores & electrónica',
                                            'computadore & electronica',
                                            'computadores & electr�nica',
                                            'computadores y electronica',
                                            'futura mamá',
                                            'futura mam�',
                                            'futura mama bebes y ninos',
                                            'futura mamá bebés y niños',
                                            'futura mamá, bebés y niños',
                                            'futura mamá,bebés y niños',
                                            'futura mam� beb�s y ni�os',
                                            'futura mam�,beb�s y ni�os',
                                            'hogar',
                                            'moda',
                                            'moda calzado belleza y salud',
                                            'moda, calzado, belleza y salud',
                                            'moda,calzado,belleza y salud',
                                            'otros',
                                            'otros productos',
                                            'other',
                                            'tiempo libre')
            ) b
        using (environment_id)
        """.format(params.get_date_from())
        return query


    def query_buyer_return_over_past(self, params: ReadParams) -> str:
        """
        Method return str with query
        """
        query = """
        select 
            date '{0}' dt_metric,
            (date '{0}' - interval '60' day) first_period_start,
            (date '{0}' - interval '31' day) first_period_end,
            (date '{0}' - interval '30' day) second_period_start,
            (date '{0}' - interval '1' day) second_period_end,
            count(distinct case when b.environment_id is not null
                then b.environment_id end) buyers_first_period,
            count(distinct case when a.environment_id is not null
                then a.environment_id end) buyers_second_period,
            count(distinct case when a.environment_id is not null 
                and b.environment_id is not null
                then a.environment_id end) buyers_both_periods
        from
        (--second period
        select distinct 
            environment_id
        from 
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where 
            client_id = 'yapocl' and event_type in 
                ('Call','SMS','Send','Show')
            and cast(date_parse(cast(year as varchar) || '-' ||
                cast(month as varchar) || '-' ||
                cast(day as varchar),'%Y-%c-%e') as date)
            between (date '{0}' - interval '30' day)
                and (date '{0}' - interval '1' day)
            and local_main_category in ('computadores & electrónica',
                                        'computadore & electronica',
                                        'computadores & electr�nica',
                                        'computadores y electronica',
                                        'futura mamá',
                                        'futura mam�',
                                        'futura mama bebes y ninos',
                                        'futura mamá bebés y niños',
                                        'futura mamá, bebés y niños',
                                        'futura mamá,bebés y niños',
                                        'futura mam� beb�s y ni�os',
                                        'futura mam�,beb�s y ni�os',
                                        'hogar','moda',
                                        'moda calzado belleza y salud',
                                        'moda, calzado, belleza y salud',
                                        'moda,calzado,belleza y salud',
                                        'otros',
                                        'otros productos',
                                        'other',
                                        'tiempo libre')
        ) a
        full join 
        (--first period
        select distinct 
            environment_id
        from 
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where 
            client_id = 'yapocl' and event_type in ('Call','SMS','Send','Show')
            and cast(date_parse(cast(year as varchar) || '-' ||
                cast(month as varchar) || '-' ||
                cast(day as varchar),'%Y-%c-%e') as date)
            between (date '{0}' - interval '60' day)
            and (date '{0}' - interval '31' day)
            and local_main_category in ('computadores & electrónica',
                                        'computadore & electronica',
                                        'computadores & electr�nica',
                                        'computadores y electronica',
                                        'futura mamá',
                                        'futura mam�',
                                        'futura mama bebes y ninos',
                                        'futura mamá bebés y niños',
                                        'futura mamá, bebés y niños',
                                        'futura mamá,bebés y niños',
                                        'futura mam� beb�s y ni�os',
                                        'futura mam�,beb�s y ni�os',
                                        'hogar','moda',
                                        'moda calzado belleza y salud',
                                        'moda, calzado, belleza y salud',
                                        'moda,calzado,belleza y salud',
                                        'otros',
                                        'otros productos',
                                        'other',
                                        'tiempo libre')
        ) b
        using (environment_id)
        """.format(params.get_date_from())
        return query

    def delete_current(self, params: ReadParams) -> str:
        """
        Method that returns delete data from one day
        """
        command = """
                    delete from dm_peak.buyer_return_over_current where 
                    dt_metric::date = 
                    '""" + params.get_date_from() + """'::date """

        return command


    def delete_past(self, params: ReadParams) -> str:
        """
        Method that returns delete data from one day
        """
        command = """
                    delete from dm_peak.buyer_return_over_past where 
                    dt_metric::date = 
                    '""" + params.get_date_from() + """'::date """
        return command
