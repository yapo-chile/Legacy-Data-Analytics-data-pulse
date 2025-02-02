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

    def query_base_postgresql_blocket(self) -> str:
        """
        Method return str with query
        """
        query = """
        select '2'
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

    def get_pulse_partners_leads(self, chunk) -> str:
        """
        Method return str with query
        """

        listIdsStr = "'" + "','".join([str(x) for x in chunk]) + "'"

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
            and split_part(ad_id,':',4) in ({1})
        group by 1,2
        order by 1,2
        """.format(self.params.get_date_from(), listIdsStr)
        return queryAthena

    def get_partner_ads(self) -> str:
        """
        Method return str with query
        """
        queryBlocket = """
        select distinct
            d.ad_id,
            d.list_id,
            d.value as integrador
        from
            (--d
            select distinct
                ad_id,
                list_id,
                value
            from
                blocket_{0}.ad_params ap
            left join blocket_{0}.ads using(ad_id)
            where
                ap.name = 'link_type'
            union all
            select distinct
                ad_id,
                list_id,
                value
            from
                blocket_{1}.ad_params ap
            left join blocket_{1}.ads using(ad_id)
            where
                ap.name = 'link_type'
            union all
            select distinct
                ad_id,
                list_id,
                value
            from
                public.ad_params ap
            left join public.ads using(ad_id)
            where
                ap.name = 'link_type'
            ) d
        """.format(self.params.get_last_year(), self.params.get_current_year())
        return queryBlocket

    def get_partner_ads_params(self, chunk) -> str:
        """
        Method return str with query
        """

        adIdsStr = ",".join([str(x) for x in chunk])

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
        where
            a.ad_id in ({2})
        group by 1
        """.format(self.params.get_last_year(),
                   self.params.get_current_year(),
                   adIdsStr)
        return queryBlocket

    def get_partner_ad_info(self, chunk) -> str:
        """
        Method return str with query
        """

        adIdsStr = ",".join([str(x) for x in chunk])

        queryBlocket = """
        select distinct 
            c.ad_id,
            --c.list_id,
            c.list_time::date,
            c.deletion_date::date,
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
                case when status = 'deleted' then modified_at
                     else null
                end deletion_date,
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
                case when status = 'deleted' then modified_at
                     else null
                end deletion_date,
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
                case when status = 'deleted' then modified_at
                     else null
                end deletion_date,
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
        where
            c.ad_id in ({4})
        """.format(self.params.get_date_from(),
                   self.params.get_date_to(),
                   self.params.get_last_year(),
                   self.params.get_current_year(),
                   adIdsStr)
        return queryBlocket

    def get_partner_users(self, chunk) -> str:
        """
        Method return str with query
        """

        userIdStr = ",".join([str(x) for x in set(list(chunk))])

        queryBlocket = """
        select distinct
            user_id,
            email
        from
            public.users
        where
            user_id in ({0})
        """.format(userIdStr)
        return queryBlocket

    def ad_inmo_params(self, chunk) -> str:
        """
        Method return str with query
        """

        adIdsStr = ",".join([str(x) for x in chunk])

        queryBlocket = """
        select distinct
            z.ad_id,
            z.rooms,
            z.squared_meters,
            z.estate_type,
            z.currency
        from
            (select distinct
                ad_id,
                max((case when ap."name" = 'rooms' then ap.value::int else null end) ) rooms,
                max((case when ap."name" = 'size' then ap.value::int else null end) ) squared_meters,
                max((case when ap."name" = 'estate_type' then ap.value::int else null end) ) estate_type,
                max((case when ap."name" = 'currency' then ap.value else null end) ) currency
            from
                public.ad_params ap
            where
                ap."name" in ('rooms','size', 'estate_type','currency')
                and ad_id in ({2})
            group by 1
            union all
            select distinct
                ad_id,
                max((case when ap."name" = 'rooms' then ap.value::int else null end) ) rooms,
                max((case when ap."name" = 'size' then ap.value::int else null end) ) size,
                max((case when ap."name" = 'estate_type' then ap.value::int else null end) ) estate_type,
                max((case when ap."name" = 'currency' then ap.value else null end) ) currency
            from
                blocket_{0}.ad_params ap
            where
                ap."name" in ('rooms','size', 'estate_type','currency')
                and ad_id in ({2})
            group by 1
            union all
            select distinct
                ad_id,
                max((case when ap."name" = 'rooms' then ap.value::int else null end) ) rooms,
                max((case when ap."name" = 'size' then ap.value::int else null end) ) size,
                max((case when ap."name" = 'estate_type' then ap.value::int else null end) ) estate_type,
                max((case when ap."name" = 'currency' then ap.value else null end) ) currency
            from
                blocket_{1}.ad_params ap
            where
                ap."name" in ('rooms','size', 'estate_type','currency')
                and ad_id in ({2})
            group by 1)z 
        where 
            z.estate_type is not null
        """.format(self.params.get_last_year(),
                   self.params.get_current_year(),
                   adIdsStr)
        return queryBlocket

    def ad_car_params(self, chunk) -> str:
        """
        Method return str with query
        """

        adIdsStr = ",".join([str(x) for x in chunk])

        queryBlocket = """
        select distinct
            ad_id,
            car_year,
            brand,
            model,
            km
        from
            (select distinct
                ad_id,
                max((case when ap."name" = 'regdate' then ap.value::int else null end) ) car_year,
                max((case when ap."name" = 'brand' then ap.value::int else null end) ) brand,
                max((case when ap."name" = 'model' then ap.value::int else null end) ) model,
                max((case when ap."name" = 'mileage' then ap.value::int else null end) ) km
            from
                public.ad_params ap
            where
                ap."name" in ('regdate','brand', 'model','mileage')
                and ad_id in ({2})
            group by 1
            union all
            select distinct
                ad_id,
                max((case when ap."name" = 'regdate' then ap.value::int else null end) ) car_year,
                max((case when ap."name" = 'brand' then ap.value::int else null end) ) brand,
                max((case when ap."name" = 'model' then ap.value::int else null end) ) model,
                max((case when ap."name" = 'mileage' then ap.value::int else null end) ) km
            from
                blocket_{0}.ad_params ap
            where
                ap."name" in ('regdate','brand', 'model','mileage')
                and ad_id in ({2})
            group by 1
            union all
            select distinct
                ad_id,
                max((case when ap."name" = 'regdate' then ap.value::int else null end) ) car_year,
                max((case when ap."name" = 'brand' then ap.value::int else null end) ) brand,
                max((case when ap."name" = 'model' then ap.value::int else null end) ) model,
                max((case when ap."name" = 'mileage' then ap.value::int else null end) ) km
            from
                blocket_{1}.ad_params ap
            where
                ap."name" in ('regdate','brand', 'model','mileage')
                and ad_id in ({2})
            group by 1)z 
        where 
            brand is not null
        """.format(self.params.get_last_year(),
                   self.params.get_current_year(),
                   adIdsStr)
        return queryBlocket

    def delete_base(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from ods.partners_leads where 
                    timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command
