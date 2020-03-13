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

    def query_base_postgresql_dw(self) -> str:
        """
        Method return str with query
        """
        query = """
                select '1'
            """
        return query

    def query_base_pulse(self) -> str:
        """
        Method return str with query
        """
        query = """
        select '3'
        """
        return query

    def get_pulse_partners_leads(self) -> str:
        """
        Method return str with query
        cast('2020-03-04' as date)
        """
        queryAthena = """
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
        return queryAthena

    def get_partner_ads(self) -> str:
        """
        Method return str with query
        """
        queryBlocket = """
        select distinct
            d.ad_id,
            d.value as integrador
        from
            (--d
            select distinct
                ad_id,
                value
            from
                blocket_{0}.ad_params
            where
                name = 'link_type'
            union all
            select distinct
                ad_id,
                value
            from
                blocket_{1}.ad_params
            where
                name = 'link_type'
            union all
            select distinct
                ad_id,
                value
            from
                public.ad_params
            where
                name = 'link_type'
            ) d
        """.format(self.params.get_last_year(), self.params.get_current_year())
        return queryBlocket

    def get_partner_ads_params(self) -> str:
        """
        Method return str with query
        """
        queryBlocket = """
        select distinct
            a.ad_id,
            max(a.patente) patente,
            max(a.codigo_inmo) codigo_inmo,
            max(a.communes) comuna
        from 
            (--a
            select distinct
                ad_id,
                case when name = 'plates' then value
                end patente,
                case when name = 'ext_code' then value
                end codigo_inmo,
                case when name = 'communes' then value
                end communes
            from
                blocket_{0}.ad_params
            where
                name in ('plates','ext_code','communes')
            union all
            select distinct
                ad_id,
                case when name = 'plates' then value
                end patente,
                case when name = 'ext_code' then value
                end codigo_inmo,
                case when name = 'communes' then value
                end communes
            from
                blocket_{1}.ad_params
            where
                name in ('plates','ext_code','communes')
            union all
            select distinct
                ad_id,
                case when name = 'plates' then value
                end patente,
                case when name = 'ext_code' then value
                end codigo_inmo,
                case when name = 'communes' then value
                end communes
            from
                public.ad_params
            where
                name in ('plates','ext_code','communes')
            ) a
        group by 1
        """.format(self.params.get_last_year(), self.params.get_current_year())
        return queryBlocket

    def get_partner_ad_info(self) -> str:
        """
        Method return str with query
        """
        queryBlocket = """
        select distinct 
            c.ad_id,
            c.list_id,
            c.list_time::date,
            case when c.category in (2020,2040,2060,2080,2100,2120) then 'Motor'
                 when c.category in (1220,1240,1260) then 'Real Estate'
            end vertical,
            c.category,
            c.region,
            c.price,
            c.name sucursal,
            c.user_id
        from
            (
            select distinct 
                ad_id,
                list_id,
                list_time,
                category,
                region,
                price,
                name,
                user_id
            from
                blocket_{2}.ads
            where
                category in (2020,2040,2060,2080,2100,2120,1220,1240,1260)
                and list_time::date between date '{0}' + interval '-7 month' and date '{1}'
            union all
            select distinct 
                ad_id,
                list_id,
                list_time,
                category,
                region,
                price,
                name,
                user_id
            from
                blocket_{3}.ads
            where
                category in (2020,2040,2060,2080,2100,2120,1220,1240,1260)
                and list_time::date between date '{0}' + interval '-7 month' and date '{1}'
            union all
            select distinct 
                ad_id,
                list_id,
                list_time,
                category,
                region,
                price,
                name,
                user_id
            from
                public.ads
            where
                category in (2020,2040,2060,2080,2100,2120,1220,1240,1260)
                and list_time::date between date '{0}' + interval '-7 month' and date '{1}'
            ) c
        """.format(self.params.get_date_from(),
                   self.params.get_date_to(),
                   self.params.get_last_year(),
                   self.params.get_current_year())
        return queryBlocket

    def get_partner_ad_deletion_date(self) -> str:
        """
        Method return str with query
        """
        queryBlocket = """
        select distinct
            ad_id,
            deletion_date::date
        from
            (--a
            select distinct
                ad_id,
                modified_at deletion_date
            from
                blocket_{2}.ads
            where
                category in (2020,2040,2060,2080,2100,2120,1220,1240,1260)
                and list_time::date between date '{0}' + interval '-7 month' and date '{1}'
                and status = 'deleted'
            union all 
            select distinct
                ad_id,
                modified_at deletion_date
            from
                blocket_{3}.ads
            where
                category in (2020,2040,2060,2080,2100,2120,1220,1240,1260)
                and list_time::date between date '{0}' + interval '-7 month' and date '{1}'
                and status = 'deleted'
            union all 
            select distinct
                ad_id,
                modified_at deletion_date
            from
                public.ads
            where
                category in (2020,2040,2060,2080,2100,2120,1220,1240,1260)
                and list_time::date between date '{0}' + interval '-7 month' and date '{1}'
                and status = 'deleted'
            ) a
        """.format(self.params.get_date_from(),
                   self.params.get_date_to(),
                   self.params.get_last_year(),
                   self.params.get_current_year())
        return queryBlocket

    def get_partner_users(self) -> str:
        """
        Method return str with query
        """
        queryBlocket = """
        select distinct
            user_id,
            email
        from
            public.users
        """
        return queryBlocket

    def ad_inmo_params(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select distinct
            ad_id_nk ad_id,
            rooms::int,
            meters::int squared_meters,
            estate_type::int,
            currency
        from
            ods.ads_inmo_params
        """
        return queryDW

    def ad_car_params(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select distinct
            ad_id_nk ad_id,
            car_year::int,
            brand::int,
            model::int,
            mileage::int km
        from
            ods.ads_cars_params
        """
        return queryDW

    def delete_base(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_analysis.test_partners_leads where 
                    timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command
