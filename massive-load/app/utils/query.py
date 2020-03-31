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

    def get_enrich_partner_ads_monthly(self) -> str:
        """
        Method return str with query to load a historichal
        table with 7 months of history ads
        """
        queryBlocket = """
        with tmp_users as (
            select distinct
	            user_id,
	            email
            from
	            public.users), tmp_ads_params as (
            select distinct
	            a.ad_id,
                max(a.patente) patente,
                max(a.codigo_inmo) codigo_inmo,
                max(a.communes) comuna,
                max(a.integrador) integrador
            from 
                (select distinct
                ad_id,
                case when name = 'plates' then value
                end patente,
                case when name = 'ext_code' then value
                end codigo_inmo,
                case when name = 'communes' then value
                end communes,
                case when name = 'link_type' then value
                end integrador
            from
                blocket_2019.ad_params
            where
                name in ('plates','ext_code','communes','link_type')
            union all
            select distinct
                ad_id,
                case when name = 'plates' then value
                end patente,
                case when name = 'ext_code' then value
                end codigo_inmo,
                case when name = 'communes' then value
                end communes,
                case when name = 'link_type' then value
                end integrador
            from
                blocket_2020.ad_params
            where
                name in ('plates','ext_code','communes','link_type')
            union all
            select distinct
	            ad_id,
                case when name = 'plates' then value
                end patente,
                case when name = 'ext_code' then value
                end codigo_inmo,
                case when name = 'communes' then value
                end communes,
                case when name = 'link_type' then value
                end integrador
            from
                public.ad_params
            where
                name in ('plates','ext_code','communes','link_type')) a
            group by 1), tmp_ads_monthly as (
            select distinct 
	            c.ad_id,
                c.list_id,
                c.list_time::date,
                c.vertical,
                c.category,
                c.region,
                c.price,
                c.name,
                c.user_id,
                c.email,
                c.modified_at::date,
                c.status,
                c.patente,
                c.codigo_inmo,
                c.comuna,
                c.integrador
            from
	            (select distinct 
		            a.ad_id,
    	            a.list_id,
    	            a.list_time,
                    case when a.category in (2020,2040,2060,2080,2100,2120) then 'Motor'
                        when a.category in (1220,1240,1260) then 'Real Estate'
                    end vertical,
    	            a.category,
    	            a.region,
    	            a.price,
    	            a.name,
    	            a.user_id,
    	            u.email,
    	            a.modified_at,
    	            a.status,
    	            p.patente,
    	            p.codigo_inmo,
    	            p.comuna,
    	            p.integrador
	            from
                    blocket_2019.ads a
                    left join tmp_users u on (u.user_id = a.user_id)
                    left join tmp_ads_params p on (p.ad_id = a.ad_id)
	            where
                    a.category in (2020,2040,2060,2080,2100,2120,1220,1240,1260)
                    and a.list_time::date between date '2020-01-01' and date '2020-01-31'
                union all
                select distinct 
                    a.ad_id,
                    a.list_id,
                    a.list_time,
                    case when a.category in (2020,2040,2060,2080,2100,2120) then 'Motor'
                         when a.category in (1220,1240,1260) then 'Real Estate'
                    end vertical,
                    a.category,
                    a.region,
                    a.price,
                    a.name,
                    a.user_id,
                    u.email,
                    a.modified_at,
                    a.status,
    	            p.patente,
    	            p.codigo_inmo,
    	            p.comuna,
    	            p.integrador
                from
                    blocket_2020.ads a
                    left join tmp_users u on (u.user_id = a.user_id)
                    left join tmp_ads_params p on (p.ad_id = a.ad_id)
                where
                    a.category in (2020,2040,2060,2080,2100,2120,1220,1240,1260)
                    and a.list_time::date between date '2020-01-01' and date '2020-01-31'
                union all
                select distinct 
                    a.ad_id,
                    a.list_id,
                    a.list_time,
                    case when a.category in (2020,2040,2060,2080,2100,2120) then 'Motor'
                         when a.category in (1220,1240,1260) then 'Real Estate'
                    end vertical,
                    a.category,
                    a.region,
                    a.price,
                    a.name,
                    a.user_id,
                    u.email,
                    a.modified_at,
                    a.status,
    	            p.patente,
    	            p.codigo_inmo,
    	            p.comuna,
    	            p.integrador
                from
                    public.ads a
                    left join tmp_users u on (u.user_id = a.user_id)
                    left join tmp_ads_params p on (p.ad_id = a.ad_id)
                where
                    a.category in (2020,2040,2060,2080,2100,2120,1220,1240,1260)
                    and a.list_time::date between date '2020-01-01' and date '2020-01-31') c)
            select
                c.ad_id,
                c.list_id::text,
                c.list_time,
                c.vertical,
                c.category,
                c.region,
                c.price::text,
                c.name,
                c.user_id,
                c.email,
                c.modified_at,
                c.status,
                c.patente,
                c.codigo_inmo,
                c.comuna,
                c.integrador
            from 
                tmp_ads_monthly c
        """
        return queryBlocket

    def get_enrich_partner_ads_daily(self) -> str:
        """
        Method return str with query to load a historichal
        table with 7 months (windowed) of history ads
        """
        queryBlocket = """
        with tmp_users as (
            select distinct
	            user_id,
	            email
            from
	            public.users), tmp_ads_params as (
            select distinct
	            a.ad_id,
                max(a.patente) patente,
                max(a.codigo_inmo) codigo_inmo,
                max(a.communes) comuna,
                max(a.integrador) integrador
            from 
                (select distinct
	            ad_id,
                case when name = 'plates' then value
                end patente,
                case when name = 'ext_code' then value
                end codigo_inmo,
                case when name = 'communes' then value
                end communes,
                case when name = 'link_type' then value
                end integrador
            from
                public.ad_params
            where
                name in ('plates','ext_code','communes','link_type')) a
            group by 1), tmp_ads_monthly as (
            select distinct 
	            c.ad_id,
                c.list_id,
                c.list_time::date,
                c.vertical,
                c.category,
                c.region,
                c.price,
                c.name,
                c.user_id,
                c.email,
                c.modified_at::date,
                c.status,
                c.patente,
                c.codigo_inmo,
                c.comuna,
                c.integrador
            from
	            (select distinct 
                    a.ad_id,
                    a.list_id,
                    a.list_time,
                    case when a.category in (2020,2040,2060,2080,2100,2120) then 'Motor'
                         when a.category in (1220,1240,1260) then 'Real Estate'
                    end vertical,
                    a.category,
                    a.region,
                    a.price,
                    a.name,
                    a.user_id,
                    u.email,
                    a.modified_at,
                    a.status,
    	            p.patente,
    	            p.codigo_inmo,
    	            p.comuna,
    	            p.integrador
                from
                    public.ads a
                    left join tmp_users u on (u.user_id = a.user_id)
                    left join tmp_ads_params p on (p.ad_id = a.ad_id)
                where
                    a.category in (2020,2040,2060,2080,2100,2120,1220,1240,1260)
                    and a.list_time::date = date '{0}') c)
            select
                c.ad_id,
                c.list_id,
                c.list_time,
                c.vertical,
                c.category,
                c.region,
                c.price::text,
                c.name,
                c.user_id,
                c.email,
                c.modified_at,
                c.status,
                c.patente,
                c.codigo_inmo,
                c.comuna,
                c.integrador
            from 
                tmp_ads_monthly c
        """.format(self.params.get_date_from())
        return queryBlocket

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
        queryDW = """
        select distinct
            ad_id,
            list_id,
            list_time,
            vertical,
            category,
            region,
            price,
            name as sucursal,
            user_id,
            email,
            case when status = 'deleted' then modified_at
                 else null
            end deletion_date,
            patente,
            codigo_inmo,
            comuna,
            integrador
        from dm_analysis.temp_hist_partner_ads
        """
        return queryDW

    def ad_inmo_params(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select distinct
            ad_id_nk as ad_id,
            rooms::int,
            meters::int squared_meters,
            estate_type::int,
            currency
        from
            ods.ads_inmo_params ip
            inner join dm_analysis.temp_hist_partner_ads pa on (pa.ad_id = ip.ad_id_nk)
        """
        return queryDW

    def ad_car_params(self) -> str:
        """
        Method return str with query
        """
        queryDW = """
        select distinct
            ad_id_nk as ad_id,
            car_year::int,
            brand::int,
            model::int,
            mileage::int km
        from
            ods.ads_cars_params cp
            inner join dm_analysis.temp_hist_partner_ads pa on (pa.ad_id = cp.ad_id_nk)
        """
        return queryDW

    def delete_base_temp_hist_partner_ads_current_day(self) -> str:
        """
        Method that delete temp table
        """
        command = """
                    delete from dm_analysis.temp_hist_partner_ads where 
                    list_time::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command

    def delete_base_temp_hist_partner_ads_last_day(self) -> str:
        """
        Method that delete temp table
        """
        command = """
                    delete from dm_analysis.temp_hist_partner_ads where 
                    list_time::date = 
                    '""" + self.params.get_date_from() \
                         + """'::date + interval '-7 month' """

        return command

    def delete_base(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_analysis.test_partners_leads where 
                    timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command
