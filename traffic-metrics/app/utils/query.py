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

    def query_traffic_metrics(self) -> str:
        """
        Method return str with query
        """
        query = """
        select
            cast(date_parse(cast(year as varchar) || '-' || 
            cast(month as varchar) || '-' ||
            cast(day as varchar),'%Y-%c-%e') as date) timedate,
            'All Yapo' platform,
            'All Yapo' traffic_channel,
            'All Yapo' vertical,
            'All' main_category,
            count(distinct environment_id) active_users,
            count(distinct session_id) sessions,
            count(distinct case when event_type = 'View' and
            object_type = 'ClassifiedAd' then environment_id end)
            active_users_that_do_adviews,
            count(distinct case when event_type in ('Call','SMS','Send','Show')
            then environment_id end) active_users_that_do_leads
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
        group by 1,2,3,4,5
            union all
        select
            cast(date_parse(cast(year as varchar) || '-' || 
            cast(month as varchar) || '-' ||
            cast(day as varchar),'%Y-%c-%e') as date) timedate,
            case when (device_type = 'desktop' and (object_url like
                '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or
                product_type = 'Web' or object_url like '%www.yapo.cl%'
                or (device_type = 'desktop' and product_type = 'unknown')
            then 'Web'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and (object_url like '%www2.yapo.cl%' or object_url
                like '%//yapo.cl%')) or product_type = 'M-Site'
                or object_url like '%m.yapo.cl%' or ((device_type =
                'mobile' or device_type = 'tablet') and product_type
                = 'unknown')
            then 'MSite'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and object_url is not null and product_type = 'AndroidApp')
                or product_type = 'AndroidApp'
            then 'AndroidApp'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and object_url is not null and product_type = 'iOSApp')
                or product_type = 'iOSApp' or product_type = 'iPadApp'
            then 'iOSApp'
            end platform,
            case when traffic_channel = 'Direct' then 'Direct'
                when traffic_channel = 'SEO' then 'SEO'
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
            case when local_main_category in ('vehiculos',
                                              'vehículos',
                                              'veh�culos')
            then 'Motor'
                when local_category_level1 in ('arriendo de temporada')
                and local_main_category in ('inmuebles') then 'Holiday Rental'
                when local_category_level1 in ('ofertas de empleo') and
                local_main_category in ('servicios',
                                        'servicios negocios y empleo',
                                        'servicios, negocios y empleo',
                                        'servicios,negocios y empleo')
                then 'Jobs'
                when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                when local_category_level1 in ('busco empleo',
                                               'servicios',
                                               'negocios,maquinaria y construccion',
                                               'negocios maquinaria y construccion',
                                               'negocios maquinaria y construcción',
                                               'negocios maquinaria y construcci髇',
                                               'negocios maquinaria y construcci�n',
                                               'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                when local_category_level1 in ('arrendar','arriendo','comprar')
                and local_main_category in ('inmuebles') then 'Real Estate'
                when local_main_category in ('unknown','undefined')
                then 'Undefined'
                else 'Undefined'
            end vertical,
            case when local_main_category in ('computadores & electrónica',
                                              'computadore & electronica',
                                              'computadores & electr?nica',
                                              'computadores y electronica')
            then 'Computadores & electrónica'
                when local_main_category in ('futura mamá',
                                             'futura mam?',
                                             'futura mama bebes y ninos',
                                             'futura mamá bebés y niños',
                                             'futura mamá, bebés y niños',
                                             'futura mamá,bebés y niños',
                                             'futura mam? beb?s y ni?os',
                                             'futura mam?,beb?s y ni?os')
            then 'Futura mamá, bebés y niños'
                when local_main_category in ('hogar') then 'Hogar'
                when local_main_category in ('inmuebles') then 'Inmuebles'
                when local_main_category in ('moda',
                                             'moda calzado belleza y salud',
                                             'moda, calzado, belleza y salud',
                                             'moda,calzado,belleza y salud')
            then 'Moda, calzado, belleza y salud'
                when local_main_category in ('otros',
                                             'otros productos',
                                             'other')
            then 'Otros'
                when local_main_category in ('servicios',
                                             'servicios negocios y empleo',
                                             'servicios, negocios y empleo',
                                             'servicios,negocios y empleo')
            then 'Servicios, negocios y empleo'
                when local_main_category in ('tiempo libre')
            then 'Tiempo libre'
                when local_main_category in ('vehiculos',
                                             'vehículos',
                                             'veh?culos',
                                             'vehï¿œculos')
            then 'Vehículos'
                when local_main_category in ('unknown','undefined')
                then 'Undefined'
                when local_main_category is null then 'Undefined'
                else 'Undefined'
            end main_category,
            count(distinct environment_id) active_users,
            count(distinct session_id) sessions,
            count(distinct case when event_type = 'View' and
            object_type = 'ClassifiedAd' then environment_id end)
            active_users_that_do_adviews,
            count(distinct case when event_type in ('Call','SMS','Send','Show')
            then environment_id end) active_users_that_do_leads
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
            and product_type is not null
        group by 1,2,3,4,5
            union all
        select
            cast(date_parse(cast(year as varchar) || '-' || 
            cast(month as varchar) || '-' ||
            cast(day as varchar),'%Y-%c-%e') as date) timedate,
            case when (device_type = 'desktop' and (object_url like
            '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or
            product_type = 'Web' or object_url like '%www.yapo.cl%'
            or (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and (object_url like '%www2.yapo.cl%' or object_url
                like '%//yapo.cl%')) or product_type = 'M-Site'
                or object_url like '%m.yapo.cl%' or ((device_type =
                'mobile' or device_type = 'tablet') and product_type
                = 'unknown')
            then 'MSite'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and object_url is not null and product_type = 'AndroidApp')
                or product_type = 'AndroidApp'
            then 'AndroidApp'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and object_url is not null and product_type = 'iOSApp')
                or product_type = 'iOSApp' or product_type = 'iPadApp'
            then 'iOSApp'
            end platform,
            case when traffic_channel = 'Direct' then 'Direct'
                when traffic_channel = 'SEO' then 'SEO'
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
            case when local_main_category in ('vehiculos',
                                              'vehículos',
                                              'veh�culos')
            then 'Motor' 
                when local_category_level1 in ('arriendo de temporada')
                and local_main_category in ('inmuebles') then 'Holiday Rental' 
                when local_category_level1 in ('ofertas de empleo') and
                local_main_category in ('servicios',
                                        'servicios negocios y empleo',
                                        'servicios, negocios y empleo',
                                        'servicios,negocios y empleo')
                then 'Jobs'
                when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                when local_category_level1 in ('busco empleo',
                                               'servicios',
                                               'negocios,maquinaria y construccion',
                                               'negocios maquinaria y construccion',
                                               'negocios maquinaria y construcción',
                                               'negocios maquinaria y construcci髇',
                                               'negocios maquinaria y construcci�n',
                                               'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                when local_category_level1 in ('arrendar','arriendo','comprar')
                and local_main_category in ('inmuebles') then 'Real Estate'
                when local_main_category in ('unknown','undefined')
                then 'Undefined'
                else 'Undefined'
            end vertical,
            'All' main_category,
            count(distinct environment_id) active_users,
            count(distinct session_id) sessions,
            count(distinct case when event_type = 'View' and
            object_type = 'ClassifiedAd' then environment_id end)
            active_users_that_do_adviews,
            count(distinct case when event_type in ('Call','SMS','Send','Show')
            then environment_id end) active_users_that_do_leads
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
            and product_type is not null
        group by 1,2,3,4,5
            union all
        select
            cast(date_parse(cast(year as varchar) || '-' || 
            cast(month as varchar) || '-' ||
            cast(day as varchar),'%Y-%c-%e') as date) timedate, 
            'All Yapo' platform,
            case when traffic_channel = 'Direct' then 'Direct'
                when traffic_channel = 'SEO' then 'SEO'
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
            case when local_main_category in ('vehiculos',
                                              'vehículos',
                                              'veh�culos')
            then 'Motor' 
                when local_category_level1 in ('arriendo de temporada')
                and local_main_category in ('inmuebles') then 'Holiday Rental' 
                when local_category_level1 in ('ofertas de empleo') and
                local_main_category in ('servicios',
                                        'servicios negocios y empleo',
                                        'servicios, negocios y empleo',
                                        'servicios,negocios y empleo')
                then 'Jobs'
                when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                when local_category_level1 in ('busco empleo',
                                               'servicios',
                                               'negocios,maquinaria y construccion',
                                               'negocios maquinaria y construccion',
                                               'negocios maquinaria y construcción',
                                               'negocios maquinaria y construcci髇',
                                               'negocios maquinaria y construcci�n',
                                               'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                when local_category_level1 in ('arrendar','arriendo','comprar')
                and local_main_category in ('inmuebles') then 'Real Estate'
                when local_main_category in ('unknown','undefined')
                then 'Undefined'
                else 'Undefined'
            end vertical,
            case when local_main_category in ('computadores & electrónica',
                                              'computadore & electronica',
                                              'computadores & electr?nica',
                                              'computadores y electronica')
            then 'Computadores & electrónica'
                when local_main_category in ('futura mamá',
                                             'futura mam?',
                                             'futura mama bebes y ninos',
                                             'futura mamá bebés y niños',
                                             'futura mamá, bebés y niños',
                                             'futura mamá,bebés y niños',
                                             'futura mam? beb?s y ni?os',
                                             'futura mam?,beb?s y ni?os')
            then 'Futura mamá, bebés y niños'
                when local_main_category in ('hogar') then 'Hogar'
                when local_main_category in ('inmuebles') then 'Inmuebles'
                when local_main_category in ('moda',
                                             'moda calzado belleza y salud',
                                             'moda, calzado, belleza y salud',
                                             'moda,calzado,belleza y salud')
            then 'Moda, calzado, belleza y salud'
                when local_main_category in ('otros',
                                             'otros productos',
                                             'other')
            then 'Otros'
                when local_main_category in ('servicios',
                                             'servicios negocios y empleo',
                                             'servicios, negocios y empleo',
                                             'servicios,negocios y empleo')
            then 'Servicios, negocios y empleo'
                when local_main_category in ('tiempo libre')
            then 'Tiempo libre'
                when local_main_category in ('vehiculos',
                                             'vehículos',
                                             'veh?culos',
                                             'vehï¿œculos')
            then 'Vehículos'
                when local_main_category in ('unknown','undefined')
                then 'Undefined'
                when local_main_category is null then 'Undefined'
                else 'Undefined'
            end main_category,   
            count(distinct environment_id) active_users,
            count(distinct session_id) sessions,
            count(distinct case when event_type = 'View' and
            object_type = 'ClassifiedAd' then environment_id end)
            active_users_that_do_adviews,
            count(distinct case when event_type in ('Call','SMS','Send','Show')
            then environment_id end) active_users_that_do_leads
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
            and product_type is not null
        group by 1,2,3,4,5
            union all
        select
            cast(date_parse(cast(year as varchar) || '-' || 
            cast(month as varchar) || '-' ||
            cast(day as varchar),'%Y-%c-%e') as date) timedate,  
            'All Yapo' platform,
            case when traffic_channel = 'Direct' then 'Direct'
                when traffic_channel = 'SEO' then 'SEO'
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
            case when local_main_category in ('vehiculos',
                                              'vehículos',
                                              'veh�culos')
            then 'Motor' 
                when local_category_level1 in ('arriendo de temporada')
                and local_main_category in ('inmuebles') then 'Holiday Rental' 
                when local_category_level1 in ('ofertas de empleo') and
                local_main_category in ('servicios',
                                        'servicios negocios y empleo',
                                        'servicios, negocios y empleo',
                                        'servicios,negocios y empleo')
                then 'Jobs'
                when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                when local_category_level1 in ('busco empleo',
                                               'servicios',
                                               'negocios,maquinaria y construccion',
                                               'negocios maquinaria y construccion',
                                               'negocios maquinaria y construcción',
                                               'negocios maquinaria y construcci髇',
                                               'negocios maquinaria y construcci�n',
                                               'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                when local_category_level1 in ('arrendar','arriendo','comprar')
                and local_main_category in ('inmuebles') then 'Real Estate'
                when local_main_category in ('unknown','undefined')
                then 'Undefined'
                else 'Undefined'
            end vertical,
            'All' main_category,   
            count(distinct environment_id) active_users,
            count(distinct session_id) sessions,
            count(distinct case when event_type = 'View' and
            object_type = 'ClassifiedAd' then environment_id end)
            active_users_that_do_adviews,
            count(distinct case when event_type in ('Call','SMS','Send','Show')
            then environment_id end) active_users_that_do_leads
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
            and product_type is not null
        group by 1,2,3,4,5
            union all
        select
            cast(date_parse(cast(year as varchar) || '-' || 
            cast(month as varchar) || '-' ||
            cast(day as varchar),'%Y-%c-%e') as date) timedate,  
            'All Yapo' platform,
            'All Yapo' traffic_channel,
            case when local_main_category in ('computadores & electrónica',
                                              'computadore & electronica',
                                              'computadores & electr?nica',
                                              'computadores y electronica',
                                              'futura mamá','futura mam?',
                                              'futura mama bebes y ninos',
                                              'futura mamá bebés y niños',
                                              'futura mamá, bebés y niños',
                                              'futura mamá,bebés y niños',
                                              'futura mam? beb?s y ni?os',
                                              'futura mam?,beb?s y ni?os','
                                              hogar',
                                              'moda',
                                              'moda calzado belleza y salud',
                                              'moda, calzado, belleza y salud',
                                              'moda,calzado,belleza y salud',
                                              'otros',
                                              'otros productos',
                                              'other',
                                              'tiempo libre')
            then 'Consumer Goods'
                when local_main_category in ('inmuebles') then 'Real Estate'
                when local_main_category in ('servicios',
                                             'servicios negocios y empleo',
                                             'servicios, negocios y empleo',
                                             'servicios,negocios y empleo')
            then 'Jobs'
                when local_main_category in ('vehiculos',
                                             'vehículos',
                                             'veh?culos')
            then 'Motor'
                when local_main_category in ('unknown','undefined')
                then 'Undefined'
                else 'Undefined'
            end vertical,
            case when local_main_category in ('computadores & electrónica',
                                              'computadore & electronica',
                                              'computadores & electr?nica',
                                              'computadores y electronica')
            then 'Computadores & electrónica'
                when local_main_category in ('futura mamá',
                                             'futura mam?',
                                             'futura mama bebes y ninos',
                                             'futura mamá bebés y niños',
                                             'futura mamá, bebés y niños',
                                             'futura mamá,bebés y niños',
                                             'futura mam? beb?s y ni?os',
                                             'futura mam?,beb?s y ni?os')
            then 'Futura mamá, bebés y niños'
                when local_main_category in ('hogar') then 'Hogar'
                when local_main_category in ('inmuebles') then 'Inmuebles'
                when local_main_category in ('moda',
                                             'moda calzado belleza y salud',
                                             'moda, calzado, belleza y salud',
                                             'moda,calzado,belleza y salud')
            then 'Moda, calzado, belleza y salud'
                when local_main_category in ('otros',
                                             'otros productos',
                                             'other')
            then 'Otros'
                when local_main_category in ('servicios',
                                             'servicios negocios y empleo',
                                             'servicios, negocios y empleo',
                                             'servicios,negocios y empleo')
            then 'Servicios, negocios y empleo'
                when local_main_category in ('tiempo libre')
            then 'Tiempo libre'
                when local_main_category in ('vehiculos',
                                             'vehículos',
                                             'veh?culos',
                                             'vehï¿œculos')
            then 'Vehículos'
                when local_main_category in ('unknown','undefined')
                then 'Undefined'
                when local_main_category is null then 'Undefined'
                else 'Undefined'
            end main_category,
            count(distinct environment_id) active_users,
            count(distinct session_id) sessions,
            count(distinct case when event_type = 'View' and
            object_type = 'ClassifiedAd' then environment_id end)
            active_users_that_do_adviews,
            count(distinct case when event_type in ('Call','SMS','Send','Show')
            then environment_id end) active_users_that_do_leads
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
        group by 1,2,3,4,5
            union all
        select
            cast(date_parse(cast(year as varchar) || '-' || 
            cast(month as varchar) || '-' ||
            cast(day as varchar),'%Y-%c-%e') as date) timedate,  
            'All Yapo' platform,
            'All Yapo' traffic_channel,
            case when local_main_category in ('vehiculos',
                                              'vehículos',
                                              'veh�culos')
            then 'Motor' 
                when local_category_level1 in ('arriendo de temporada')
                and local_main_category in ('inmuebles') then 'Holiday Rental'
                when local_category_level1 in ('ofertas de empleo') and
                local_main_category in ('servicios',
                                        'servicios negocios y empleo',
                                        'servicios, negocios y empleo',
                                        'servicios,negocios y empleo')
                then 'Jobs'
                when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                when local_category_level1 in ('busco empleo',
                                               'servicios',
                                               'negocios,maquinaria y construccion',
                                               'negocios maquinaria y construccion',
                                               'negocios maquinaria y construcción',
                                               'negocios maquinaria y construcci髇',
                                               'negocios maquinaria y construcci�n',
                                               'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                when local_category_level1 in ('arrendar','arriendo','comprar')
                and local_main_category in ('inmuebles') then 'Real Estate'
                when local_main_category in ('unknown','undefined')
                then 'Undefined'
                else 'Undefined'
            end vertical,
            'All' main_category,
            count(distinct environment_id) active_users,
            count(distinct session_id) sessions,
            count(distinct case when event_type = 'View' and
            object_type = 'ClassifiedAd' then environment_id end)
            active_users_that_do_adviews,
            count(distinct case when event_type in ('Call','SMS','Send','Show')
            then environment_id end) active_users_that_do_leads
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
        group by 1,2,3,4,5
            union all
        select
            cast(date_parse(cast(year as varchar) || '-' || 
            cast(month as varchar) || '-' ||
            cast(day as varchar),'%Y-%c-%e') as date) timedate,  
            'All Yapo' platform,
            case when traffic_channel = 'Direct' then 'Direct'
                when traffic_channel = 'SEO' then 'SEO'
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
            'All Yapo' vertical,
            'All' main_category,
            count(distinct environment_id) active_users,
            count(distinct session_id) sessions,
            count(distinct case when event_type = 'View' and
            object_type = 'ClassifiedAd' then environment_id end)
            active_users_that_do_adviews,
            count(distinct case when event_type in ('Call','SMS','Send','Show')
            then environment_id end) active_users_that_do_leads
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
        group by 1,2,3,4,5
            union all
        select
            cast(date_parse(cast(year as varchar) || '-' || 
            cast(month as varchar) || '-' ||
            cast(day as varchar),'%Y-%c-%e') as date) timedate,  
            case when (device_type = 'desktop' and (object_url like
            '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or
            product_type = 'Web' or object_url like '%www.yapo.cl%'
            or (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and (object_url like '%www2.yapo.cl%' or object_url
                like '%//yapo.cl%')) or product_type = 'M-Site'
                or object_url like '%m.yapo.cl%' or ((device_type =
                'mobile' or device_type = 'tablet') and product_type
                = 'unknown')
            then 'MSite'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and object_url is not null and product_type = 'AndroidApp')
                or product_type = 'AndroidApp'
            then 'AndroidApp'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and object_url is not null and product_type = 'iOSApp')
                or product_type = 'iOSApp' or product_type = 'iPadApp'
            then 'iOSApp'
            end platform,
            'All Yapo' traffic_channel,
            case when local_main_category in ('vehiculos',
                                              'vehículos',
                                              'veh�culos')
            then 'Motor' 
                when local_category_level1 in ('arriendo de temporada')
                and local_main_category in ('inmuebles') then 'Holiday Rental' 
                when local_category_level1 in ('ofertas de empleo') and
                local_main_category in ('servicios',
                                        'servicios negocios y empleo',
                                        'servicios, negocios y empleo',
                                        'servicios,negocios y empleo')
                then 'Jobs'
                when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                when local_category_level1 in ('busco empleo',
                                               'servicios',
                                               'negocios,maquinaria y construccion',
                                               'negocios maquinaria y construccion',
                                               'negocios maquinaria y construcción',
                                               'negocios maquinaria y construcci髇',
                                               'negocios maquinaria y construcci�n',
                                               'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                when local_category_level1 in ('arrendar','arriendo','comprar')
                and local_main_category in ('inmuebles') then 'Real Estate'
                when local_main_category in ('unknown','undefined')
                then 'Undefined'
                else 'Undefined'
            end vertical,
            case when local_main_category in ('computadores & electrónica',
                                              'computadore & electronica',
                                              'computadores & electr?nica',
                                              'computadores y electronica')
            then 'Computadores & electrónica'
                when local_main_category in ('futura mamá',
                                             'futura mam?',
                                             'futura mama bebes y ninos',
                                             'futura mamá bebés y niños',
                                             'futura mamá, bebés y niños',
                                             'futura mamá,bebés y niños',
                                             'futura mam? beb?s y ni?os',
                                             'futura mam?,beb?s y ni?os')
            then 'Futura mamá, bebés y niños'
                when local_main_category in ('hogar') then 'Hogar'
                when local_main_category in ('inmuebles') then 'Inmuebles'
                when local_main_category in ('moda',
                                             'moda calzado belleza y salud',
                                             'moda, calzado, belleza y salud',
                                             'moda,calzado,belleza y salud')
            then 'Moda, calzado, belleza y salud'
                when local_main_category in ('otros',
                                             'otros productos',
                                             'other')
            then 'Otros'
                when local_main_category in ('servicios',
                                             'servicios negocios y empleo',
                                             'servicios, negocios y empleo',
                                             'servicios,negocios y empleo')
            then 'Servicios, negocios y empleo'
                when local_main_category in ('tiempo libre')
            then 'Tiempo libre'
                when local_main_category in ('vehiculos',
                                             'vehículos',
                                             'veh?culos',
                                             'vehï¿œculos')
            then 'Vehículos'
                when local_main_category in ('unknown','undefined')
                then 'Undefined'
                when local_main_category is null then 'Undefined'
                else 'Undefined'
            end main_category,
            count(distinct environment_id) active_users,
            count(distinct session_id) sessions,
            count(distinct case when event_type = 'View' and
            object_type = 'ClassifiedAd' then environment_id end)
            active_users_that_do_adviews,
            count(distinct case when event_type in ('Call','SMS','Send','Show')
            then environment_id end) active_users_that_do_leads
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
            and product_type is not null
        group by 1,2,3,4,5
            union all
        select
            cast(date_parse(cast(year as varchar) || '-' || 
            cast(month as varchar) || '-' ||
            cast(day as varchar),'%Y-%c-%e') as date) timedate,  
            case when (device_type = 'desktop' and (object_url like
            '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or
            product_type = 'Web' or object_url like '%www.yapo.cl%'
            or (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and (object_url like '%www2.yapo.cl%' or object_url
                like '%//yapo.cl%')) or product_type = 'M-Site'
                or object_url like '%m.yapo.cl%' or ((device_type =
                'mobile' or device_type = 'tablet') and product_type
                = 'unknown')
            then 'MSite'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and object_url is not null and product_type = 'AndroidApp')
                or product_type = 'AndroidApp'
            then 'AndroidApp'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and object_url is not null and product_type = 'iOSApp')
                or product_type = 'iOSApp' or product_type = 'iPadApp'
            then 'iOSApp'
            end platform,
            'All Yapo' traffic_channel,
            case when local_main_category in ('vehiculos',
                                              'vehículos',
                                              'veh�culos')
            then 'Motor' 
                when local_category_level1 in ('arriendo de temporada')
                and local_main_category in ('inmuebles') then 'Holiday Rental' 
                when local_category_level1 in ('ofertas de empleo') and
                local_main_category in ('servicios',
                                        'servicios negocios y empleo',
                                        'servicios, negocios y empleo',
                                        'servicios,negocios y empleo')
                then 'Jobs'
                when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                when local_category_level1 in ('busco empleo',
                                               'servicios',
                                               'negocios,maquinaria y construccion',
                                               'negocios maquinaria y construccion',
                                               'negocios maquinaria y construcción',
                                               'negocios maquinaria y construcci髇',
                                               'negocios maquinaria y construcci�n',
                                               'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                when local_category_level1 in ('arrendar','arriendo','comprar')
                and local_main_category in ('inmuebles') then 'Real Estate'
                when local_main_category in ('unknown','undefined')
                then 'Undefined'
                else 'Undefined'
            end vertical,
            'All' main_category,
            count(distinct environment_id) active_users,
            count(distinct session_id) sessions,
            count(distinct case when event_type = 'View' and
            object_type = 'ClassifiedAd' then environment_id end)
            active_users_that_do_adviews,
            count(distinct case when event_type in ('Call','SMS','Send','Show')
            then environment_id end) active_users_that_do_leads
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
            and product_type is not null
        group by 1,2,3,4,5
            union all
        select
            cast(date_parse(cast(year as varchar) || '-' || 
            cast(month as varchar) || '-' ||
            cast(day as varchar),'%Y-%c-%e') as date) timedate,  
            case when (device_type = 'desktop' and (object_url like
            '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or
            product_type = 'Web' or object_url like '%www.yapo.cl%'
            or (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and (object_url like '%www2.yapo.cl%' or object_url
                like '%//yapo.cl%')) or product_type = 'M-Site'
                or object_url like '%m.yapo.cl%' or ((device_type =
                'mobile' or device_type = 'tablet') and product_type
                = 'unknown')
            then 'MSite'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and object_url is not null and product_type = 'AndroidApp')
                or product_type = 'AndroidApp'
            then 'AndroidApp'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and object_url is not null and product_type = 'iOSApp')
                or product_type = 'iOSApp' or product_type = 'iPadApp'
            then 'iOSApp'
            end platform,
            'All Yapo' traffic_channel,
            'All Yapo' vertical,
            'All' main_category,
            count(distinct environment_id) active_users,
            count(distinct session_id) sessions,
            count(distinct case when event_type = 'View' and
            object_type = 'ClassifiedAd' then environment_id end)
            active_users_that_do_adviews,
            count(distinct case when event_type in ('Call','SMS','Send','Show')
            then environment_id end) active_users_that_do_leads
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
            and product_type is not null
        group by 1,2,3,4,5
            union all
        select
            cast(date_parse(cast(year as varchar) || '-' || 
            cast(month as varchar) || '-' ||
            cast(day as varchar),'%Y-%c-%e') as date) timedate,    
            case when (device_type = 'desktop' and (object_url like
            '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or
            product_type = 'Web' or object_url like '%www.yapo.cl%'
            or (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and (object_url like '%www2.yapo.cl%' or object_url
                like '%//yapo.cl%')) or product_type = 'M-Site'
                or object_url like '%m.yapo.cl%' or ((device_type =
                'mobile' or device_type = 'tablet') and product_type
                = 'unknown')
            then 'MSite'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and object_url is not null and product_type = 'AndroidApp')
                or product_type = 'AndroidApp'
            then 'AndroidApp'
                when ((device_type = 'mobile' or device_type = 'tablet')
                and object_url is not null and product_type = 'iOSApp')
                or product_type = 'iOSApp' or product_type = 'iPadApp'
            then 'iOSApp'
            end platform,
            case when traffic_channel = 'Direct' then 'Direct'
                when traffic_channel = 'SEO' then 'SEO'
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
            'All Yapo' vertical,
            'All' main_category,
            count(distinct environment_id) active_users,
            count(distinct session_id) sessions,
            count(distinct case when event_type = 'View' and
            object_type = 'ClassifiedAd' then environment_id end)
            active_users_that_do_adviews,
            count(distinct case when event_type in ('Call','SMS','Send','Show')
            then environment_id end) active_users_that_do_leads
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
            and product_type is not null
        group by 1,2,3,4,5
        order by 1,2,3,4,5
        """.format(self.params.get_date_from())
        return query

    def query_buyers(self) -> str:
        """
        Method that returns buyers metrics
        """
        command = """
                select
                    timedate::date,
                    cast(sum(active_users) as integer) dau,
                    cast(sum(active_users_that_do_leads) as integer) buyers
                from
                    dm_pulse.traffic_metrics
                where
                    platform = 'All Yapo' and traffic_channel = 'All Yapo' and
                    vertical = 'Consumer Goods' and main_category = 'All'
                    and timedate = '{0}'
                group by 1
                order by 1 """.format(self.params.get_date_from())
        return command


    def query_unique_leads(self) -> str:
        """
        Method that returns buyers metrics
        """
        command = """
        select 
            timedate,
            platform,
            traffic_channel,
            vertical,
            main_category,
            sum(unique_leads) unique_leads
        from 
        (
        select
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) timedate,
            'All Yapo' platform,
            'All Yapo' traffic_channel,
            'All Yapo' vertical,
            'All' main_category,
            count(distinct row(ad_id, environment_id)) unique_leads
        from
            yapocl_databox.insights_events_behavioral_fact_layer_365d
        where
            cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
            and event_type in ('Call','SMS','Send','Show') 
            and lead_id != 'unknown'
        group by 1,2,3,4,5
        union all
            select
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) timedate,    
                case when (device_type = 'desktop' and (object_url like 
                '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or 
                product_type = 'Web' or object_url like '%www.yapo.cl%' or
                (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and (object_url like '%www2.yapo.cl%'
                    or object_url like '%//yapo.cl%')) or product_type = 'M-Site'
                    or object_url like '%m.yapo.cl%' or ((device_type = 'mobile'
                    or device_type = 'tablet') and product_type = 'unknown')
                then 'MSite'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and object_url is not null and product_type = 'AndroidApp')
                    or product_type = 'AndroidApp'
                then 'AndroidApp'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and object_url is not null and product_type = 'iOSApp')
                    or product_type = 'iOSApp' or product_type = 'iPadApp'
                then 'iOSApp'
                end platform,
                case when traffic_channel = 'Direct' then 'Direct'
                    when traffic_channel = 'SEO' then 'SEO'
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
                case when local_main_category in ('vehiculos',
                                                  'vehículos',
                                                  'veh�culos')
                then 'Motor'
                    when local_category_level1 in ('arriendo de temporada')
                    and local_main_category in ('inmuebles')
                then 'Holiday Rental'
                    when local_category_level1 in ('ofertas de empleo') and
                    local_main_category in ('servicios',
                                            'servicios negocios y empleo',
                                            'servicios, negocios y empleo',
                                            'servicios,negocios y empleo')
                then 'Jobs'
                    when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                    when local_category_level1 in ('busco empleo',
                                                   'servicios',
                                                   'negocios,maquinaria y construccion',
                                                   'negocios maquinaria y construccion',
                                                   'negocios maquinaria y construcción',
                                                   'negocios maquinaria y construcci髇',
                                                   'negocios maquinaria y construcci�n',
                                                   'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                    when local_category_level1 in ('arrendar',
                                                   'arriendo',
                                                   'comprar') 
                    and local_main_category in ('inmuebles')
                then 'Real Estate'
                    when local_main_category in ('unknown',
                                                 'undefined')
                then 'Undefined'
                    else 'Undefined'
                end vertical,
                case when local_main_category in ('computadores & electrónica',
                                                  'computadore & electronica',
                                                  'computadores & electr�nica',
                                                  'computadores y electronica')
                then 'Computadores & electrónica'
                    when local_main_category in ('futura mamá',
                                                 'futura mam�',
                                                 'futura mama bebes y ninos',
                                                 'futura mamá bebés y niños',
                                                 'futura mamá, bebés y niños',
                                                 'futura mamá,bebés y niños',
                                                 'futura mam� beb�s y ni�os',
                                                 'futura mam�,beb�s y ni�os')
                then 'Futura mamá, bebés y niños'
                    when local_main_category in ('hogar') then 'Hogar'
                    when local_main_category in ('inmuebles') then 'Inmuebles'
                    when local_main_category in ('moda',
                                                 'moda calzado belleza y salud',
                                                 'moda, calzado, belleza y salud',
                                                 'moda,calzado,belleza y salud')
                then 'Moda, calzado, belleza y salud'
                    when local_main_category in ('otros',
                                                 'otros productos',
                                                 'other')
                then 'Otros'
                    when local_main_category in ('servicios',
                                                 'servicios negocios y empleo',
                                                 'servicios, negocios y empleo',
                                                 'servicios,negocios y empleo')
                then 'Servicios, negocios y empleo'
                    when local_main_category in ('tiempo libre') then 'Tiempo libre'
                    when local_main_category in ('vehiculos','vehículos','veh�culos') then 'Vehículos'
                    when local_main_category in ('unknown',
                                                 'undefined')
                then 'Undefined'
                    when local_main_category is null then 'Undefined'
                    else local_main_category
                end main_category,
                count(distinct row(ad_id, environment_id)) unique_leads
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
                    and event_type in ('Call','SMS','Send','Show') and lead_id != 'unknown'
            group by 1,2,3,4,5
                union all
            select
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) timedate,
                case when (device_type = 'desktop' and (object_url like 
                '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or 
                product_type = 'Web' or object_url like '%www.yapo.cl%' or
                (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and (object_url like '%www2.yapo.cl%'
                    or object_url like '%//yapo.cl%')) or product_type = 'M-Site'
                    or object_url like '%m.yapo.cl%' or ((device_type = 'mobile'
                    or device_type = 'tablet') and product_type = 'unknown')
                then 'MSite'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and object_url is not null and product_type = 'AndroidApp')
                    or product_type = 'AndroidApp'
                then 'AndroidApp'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and object_url is not null and product_type = 'iOSApp')
                    or product_type = 'iOSApp' or product_type = 'iPadApp'
                then 'iOSApp'
                end platform,
                case when traffic_channel = 'Direct' then 'Direct'
                    when traffic_channel = 'SEO' then 'SEO'
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
                case  
                    when local_category_level1 in ('arriendo de temporada')
                    and local_main_category in ('inmuebles') 
                then 'Holiday Rental'
                    when local_category_level1 in ('ofertas de empleo')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                    then 'Jobs'
                    when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                    when local_category_level1 in ('busco empleo',
                                                   'servicios',
                                                   'negocios,maquinaria y construccion',
                                                   'negocios maquinaria y construccion',
                                                   'negocios maquinaria y construcción',
                                                   'negocios maquinaria y construcci髇',
                                                   'negocios maquinaria y construcci�n',
                                                   'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                    when local_category_level1 in ('arrendar',
                                                   'arriendo',
                                                   'comprar') 
                    and local_main_category in ('inmuebles')
                then 'Real Estate'
                    when local_main_category in ('unknown',
                                                 'undefined')
                then 'Undefined'
                    else 'Undefined'
                end vertical,
                'All' main_category,
                count(distinct row(ad_id, environment_id)) unique_leads
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
                    and event_type in ('Call','SMS','Send','Show') and lead_id != 'unknown'
            group by 1,2,3,4,5
                union all
            select
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) timedate,  
                'All Yapo' platform,
                case when traffic_channel = 'Direct' then 'Direct'
                    when traffic_channel = 'SEO' then 'SEO'
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
                case  
                    when local_category_level1 in ('arriendo de temporada') and local_main_category in ('inmuebles') then 'Holiday Rental' 
                    when local_category_level1 in ('ofertas de empleo')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                    then 'Jobs'
                    when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                    when local_category_level1 in ('busco empleo',
                                                   'servicios',
                                                   'negocios,maquinaria y construccion',
                                                   'negocios maquinaria y construccion',
                                                   'negocios maquinaria y construcción',
                                                   'negocios maquinaria y construcci髇',
                                                   'negocios maquinaria y construcci�n',
                                                   'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                    when local_category_level1 in ('arrendar',
                                                   'arriendo',
                                                   'comprar') 
                    and local_main_category in ('inmuebles')
                then 'Real Estate'
                    when local_main_category in ('unknown',
                                                 'undefined')
                then 'Undefined'
                    else 'Undefined'
                end vertical,
                case when local_main_category in ('computadores & electrónica',
                                                  'computadore & electronica',
                                                  'computadores & electr�nica',
                                                  'computadores y electronica')
                then 'Computadores & electrónica'
                    when local_main_category in ('futura mamá',
                                                 'futura mam�',
                                                 'futura mama bebes y ninos',
                                                 'futura mamá bebés y niños',
                                                 'futura mamá, bebés y niños',
                                                 'futura mamá,bebés y niños',
                                                 'futura mam� beb�s y ni�os',
                                                 'futura mam�,beb�s y ni�os')
                then 'Futura mamá, bebés y niños'
                    when local_main_category in ('hogar') then 'Hogar'
                    when local_main_category in ('inmuebles') then 'Inmuebles'
                    when local_main_category in ('moda',
                                                 'moda calzado belleza y salud',
                                                 'moda, calzado, belleza y salud',
                                                 'moda,calzado,belleza y salud')
                then 'Moda, calzado, belleza y salud'
                    when local_main_category in ('otros',
                                                 'otros productos',
                                                 'other')
                then 'Otros'
                    when local_main_category in ('servicios',
                                                 'servicios negocios y empleo',
                                                 'servicios, negocios y empleo',
                                                 'servicios,negocios y empleo')
                then 'Servicios, negocios y empleo'
                    when local_main_category in ('tiempo libre') then 'Tiempo libre'
                    when local_main_category in ('vehiculos','vehículos','veh�culos') then 'Vehículos'
                    when local_main_category in ('unknown',
                                                 'undefined')
                then 'Undefined'
                    when local_main_category is null then 'Undefined'
                    else local_main_category
                end main_category,   
                count(distinct row(ad_id, environment_id)) unique_leads
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
                    and event_type in ('Call','SMS','Send','Show') and lead_id != 'unknown'
            group by 1,2,3,4,5
                union all
            select
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) timedate,  
                'All Yapo' platform,
                case when traffic_channel = 'Direct' then 'Direct'
                    when traffic_channel = 'SEO' then 'SEO'
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
                case  
                    when local_category_level1 in ('arriendo de temporada') and local_main_category in ('inmuebles') then 'Holiday Rental' 
                    when local_category_level1 in ('ofertas de empleo')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                    then 'Jobs'
                    when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                    when local_category_level1 in ('busco empleo',
                                                   'servicios',
                                                   'negocios,maquinaria y construccion',
                                                   'negocios maquinaria y construccion',
                                                   'negocios maquinaria y construcción',
                                                   'negocios maquinaria y construcci髇',
                                                   'negocios maquinaria y construcci�n',
                                                   'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                    when local_category_level1 in ('arrendar',
                                                   'arriendo',
                                                   'comprar') 
                    and local_main_category in ('inmuebles')
                then 'Real Estate'
                    when local_main_category in ('unknown',
                                                 'undefined')
                then 'Undefined'
                else 'Undefined'
                end vertical,
                'All' main_category,   
                count(distinct row(ad_id, environment_id)) unique_leads
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
                    and event_type in ('Call','SMS','Send','Show') and lead_id != 'unknown'
            group by 1,2,3,4,5
                union all
            select
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) timedate,  
                'All Yapo' platform,
                'All Yapo' traffic_channel,
                case  
                    when local_category_level1 in ('arriendo de temporada') and local_main_category in ('inmuebles') then 'Holiday Rental' 
                    when local_category_level1 in ('ofertas de empleo')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                    then 'Jobs'
                    when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                    when local_category_level1 in ('busco empleo',
                                                   'servicios',
                                                   'negocios,maquinaria y construccion',
                                                   'negocios maquinaria y construccion',
                                                   'negocios maquinaria y construcción',
                                                   'negocios maquinaria y construcci髇',
                                                   'negocios maquinaria y construcci�n',
                                                   'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                    when local_category_level1 in ('arrendar',
                                                   'arriendo',
                                                   'comprar') 
                    and local_main_category in ('inmuebles')
                then 'Real Estate'
                    when local_main_category in ('unknown',
                                                 'undefined')
                then 'Undefined'
                else 'Undefined'
                end vertical,
                case when local_main_category in ('computadores & electrónica',
                                                  'computadore & electronica',
                                                  'computadores & electr�nica',
                                                  'computadores y electronica')
                then 'Computadores & electrónica'
                    when local_main_category in ('futura mamá',
                                                 'futura mam�',
                                                 'futura mama bebes y ninos',
                                                 'futura mamá bebés y niños',
                                                 'futura mamá, bebés y niños',
                                                 'futura mamá,bebés y niños',
                                                 'futura mam� beb�s y ni�os',
                                                 'futura mam�,beb�s y ni�os')
                then 'Futura mamá, bebés y niños'
                    when local_main_category in ('hogar') then 'Hogar'
                    when local_main_category in ('inmuebles') then 'Inmuebles'
                    when local_main_category in ('moda',
                                                 'moda calzado belleza y salud',
                                                 'moda, calzado, belleza y salud',
                                                 'moda,calzado,belleza y salud')
                then 'Moda, calzado, belleza y salud'
                    when local_main_category in ('otros',
                                                 'otros productos',
                                                 'other')
                then 'Otros'
                    when local_main_category in ('servicios',
                                                 'servicios negocios y empleo',
                                                 'servicios, negocios y empleo',
                                                 'servicios,negocios y empleo')
                then 'Servicios, negocios y empleo'
                    when local_main_category in ('tiempo libre') then 'Tiempo libre'
                    when local_main_category in ('vehiculos','vehículos','veh�culos') then 'Vehículos'
                    when local_main_category in ('unknown',
                                                 'undefined')
                then 'Undefined'
                    when local_main_category is null then 'Undefined'
                    else local_main_category
                end main_category,
                count(distinct row(ad_id, environment_id)) unique_leads
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
                    and event_type in ('Call','SMS','Send','Show') and lead_id != 'unknown'
            group by 1,2,3,4,5
                union all
            select
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) timedate,  
                'All Yapo' platform,
                'All Yapo' traffic_channel,
                case  
                    when local_category_level1 in ('arriendo de temporada') and local_main_category in ('inmuebles') then 'Holiday Rental' 
                    when local_category_level1 in ('ofertas de empleo')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                    then 'Jobs'
                    when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                    when local_category_level1 in ('busco empleo',
                                                   'servicios',
                                                   'negocios,maquinaria y construccion',
                                                   'negocios maquinaria y construccion',
                                                   'negocios maquinaria y construcción',
                                                   'negocios maquinaria y construcci髇',
                                                   'negocios maquinaria y construcci�n',
                                                   'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                    when local_category_level1 in ('arrendar',
                                                   'arriendo',
                                                   'comprar') 
                    and local_main_category in ('inmuebles')
                then 'Real Estate'
                    when local_main_category in ('unknown',
                                                 'undefined')
                then 'Undefined'
                    else 'Undefined'
                end vertical,
                'All' main_category,
                count(distinct row(ad_id, environment_id)) unique_leads
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
                    and event_type in ('Call','SMS','Send','Show') and lead_id != 'unknown'
            group by 1,2,3,4,5
                union all
            select
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) timedate,  
                'All Yapo' platform,
                case when traffic_channel = 'Direct' then 'Direct'
                    when traffic_channel = 'SEO' then 'SEO'
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
                'All Yapo' vertical,
                'All' main_category,
                count(distinct row(ad_id, environment_id)) unique_leads
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
                    and event_type in ('Call','SMS','Send','Show') and lead_id != 'unknown'
            group by 1,2,3,4,5
                union all
            select
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) timedate,  
                case when (device_type = 'desktop' and (object_url like 
                '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or 
                product_type = 'Web' or object_url like '%www.yapo.cl%' or
                (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and (object_url like '%www2.yapo.cl%'
                    or object_url like '%//yapo.cl%')) or product_type = 'M-Site'
                    or object_url like '%m.yapo.cl%' or ((device_type = 'mobile'
                    or device_type = 'tablet') and product_type = 'unknown')
                then 'MSite'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and object_url is not null and product_type = 'AndroidApp')
                    or product_type = 'AndroidApp'
                then 'AndroidApp'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and object_url is not null and product_type = 'iOSApp')
                    or product_type = 'iOSApp' or product_type = 'iPadApp'
                then 'iOSApp'
                end platform,
                'All Yapo' traffic_channel,
                case  
                    when local_category_level1 in ('arriendo de temporada') and local_main_category in ('inmuebles') then 'Holiday Rental' 
                    when local_category_level1 in ('ofertas de empleo')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                    then 'Jobs'
                    when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                    when local_category_level1 in ('busco empleo',
                                                   'servicios',
                                                   'negocios,maquinaria y construccion',
                                                   'negocios maquinaria y construccion',
                                                   'negocios maquinaria y construcción',
                                                   'negocios maquinaria y construcci髇',
                                                   'negocios maquinaria y construcci�n',
                                                   'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                    when local_category_level1 in ('arrendar',
                                                   'arriendo',
                                                   'comprar') 
                    and local_main_category in ('inmuebles')
                then 'Real Estate'
                    when local_main_category in ('unknown',
                                                 'undefined')
                then 'Undefined'
                    else 'Undefined'
                end vertical,
                case when local_main_category in ('computadores & electrónica',
                                                  'computadore & electronica',
                                                  'computadores & electr�nica',
                                                  'computadores y electronica')
                then 'Computadores & electrónica'
                    when local_main_category in ('futura mamá',
                                                 'futura mam�',
                                                 'futura mama bebes y ninos',
                                                 'futura mamá bebés y niños',
                                                 'futura mamá, bebés y niños',
                                                 'futura mamá,bebés y niños',
                                                 'futura mam� beb�s y ni�os',
                                                 'futura mam�,beb�s y ni�os')
                then 'Futura mamá, bebés y niños'
                    when local_main_category in ('hogar') then 'Hogar'
                    when local_main_category in ('inmuebles') then 'Inmuebles'
                    when local_main_category in ('moda',
                                                 'moda calzado belleza y salud',
                                                 'moda, calzado, belleza y salud',
                                                 'moda,calzado,belleza y salud')
                then 'Moda, calzado, belleza y salud'
                    when local_main_category in ('otros',
                                                 'otros productos',
                                                 'other')
                then 'Otros'
                    when local_main_category in ('servicios',
                                                 'servicios negocios y empleo',
                                                 'servicios, negocios y empleo',
                                                 'servicios,negocios y empleo')
                then 'Servicios, negocios y empleo'
                    when local_main_category in ('tiempo libre') then 'Tiempo libre'
                    when local_main_category in ('vehiculos','vehículos','veh�culos') then 'Vehículos'
                    when local_main_category in ('unknown',
                                                 'undefined')
                then 'Undefined'
                    when local_main_category is null then 'Undefined'
                    else local_main_category
                end main_category,
                count(distinct row(ad_id, environment_id)) unique_leads
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
                    and event_type in ('Call','SMS','Send','Show') and lead_id != 'unknown'
            group by 1,2,3,4,5
                union all
            select
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) timedate,  
                case when (device_type = 'desktop' and (object_url like 
                '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or 
                product_type = 'Web' or object_url like '%www.yapo.cl%' or
                (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and (object_url like '%www2.yapo.cl%'
                    or object_url like '%//yapo.cl%')) or product_type = 'M-Site'
                    or object_url like '%m.yapo.cl%' or ((device_type = 'mobile'
                    or device_type = 'tablet') and product_type = 'unknown')
                then 'MSite'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and object_url is not null and product_type = 'AndroidApp')
                    or product_type = 'AndroidApp'
                then 'AndroidApp'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and object_url is not null and product_type = 'iOSApp')
                    or product_type = 'iOSApp' or product_type = 'iPadApp'
                then 'iOSApp'
                end platform,
                'All Yapo' traffic_channel,
                case  
                    when local_category_level1 in ('arriendo de temporada') and local_main_category in ('inmuebles') then 'Holiday Rental' 
                    when local_category_level1 in ('ofertas de empleo')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                    then 'Jobs'
                    when local_main_category in ('computadores & electrónica',
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
                then 'Consumer Goods'
                    when local_category_level1 in ('busco empleo',
                                                   'servicios',
                                                   'negocios,maquinaria y construccion',
                                                   'negocios maquinaria y construccion',
                                                   'negocios maquinaria y construcción',
                                                   'negocios maquinaria y construcci髇',
                                                   'negocios maquinaria y construcci�n',
                                                   'negocios, maquinaria y construcción')
                    and local_main_category in ('servicios',
                                                'servicios negocios y empleo',
                                                'servicios, negocios y empleo',
                                                'servicios,negocios y empleo')
                then 'Professional Services'
                    when local_category_level1 in ('arrendar',
                                                   'arriendo',
                                                   'comprar') 
                    and local_main_category in ('inmuebles')
                then 'Real Estate'
                    when local_main_category in ('unknown',
                                                 'undefined')
                then 'Undefined'
                    else 'Undefined'
                end vertical,
                'All' main_category,
                count(distinct row(ad_id, environment_id)) unique_leads
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
                    and event_type in ('Call','SMS','Send','Show') and lead_id != 'unknown'
            group by 1,2,3,4,5
                union all
            select
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) timedate,  
                case when (device_type = 'desktop' and (object_url like 
                '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or 
                product_type = 'Web' or object_url like '%www.yapo.cl%' or
                (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and (object_url like '%www2.yapo.cl%'
                    or object_url like '%//yapo.cl%')) or product_type = 'M-Site'
                    or object_url like '%m.yapo.cl%' or ((device_type = 'mobile'
                    or device_type = 'tablet') and product_type = 'unknown')
                then 'MSite'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and object_url is not null and product_type = 'AndroidApp')
                    or product_type = 'AndroidApp'
                then 'AndroidApp'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and object_url is not null and product_type = 'iOSApp')
                    or product_type = 'iOSApp' or product_type = 'iPadApp'
                then 'iOSApp'
                end platform,
                'All Yapo' traffic_channel,
                'All Yapo' vertical,
                'All' main_category,
                count(distinct row(ad_id, environment_id)) unique_leads
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
                    and event_type in ('Call','SMS','Send','Show') and lead_id != 'unknown'
            group by 1,2,3,4,5
                union all
            select
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) timedate,  
                case when (device_type = 'desktop' and (object_url like 
                '%www2.yapo.cl%' or object_url like '%//yapo.cl%')) or 
                product_type = 'Web' or object_url like '%www.yapo.cl%' or
                (device_type = 'desktop' and product_type = 'unknown') then 'Web'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and (object_url like '%www2.yapo.cl%'
                    or object_url like '%//yapo.cl%')) or product_type = 'M-Site'
                    or object_url like '%m.yapo.cl%' or ((device_type = 'mobile'
                    or device_type = 'tablet') and product_type = 'unknown')
                then 'MSite'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and object_url is not null and product_type = 'AndroidApp')
                    or product_type = 'AndroidApp'
                then 'AndroidApp'
                    when ((device_type = 'mobile' or device_type = 'tablet')
                    and object_url is not null and product_type = 'iOSApp')
                    or product_type = 'iOSApp' or product_type = 'iPadApp'
                then 'iOSApp'
                end platform,
                case when traffic_channel = 'Direct' then 'Direct'
                    when traffic_channel = 'SEO' then 'SEO'
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
                'All Yapo' vertical,
                'All' main_category,
                count(distinct row(ad_id, environment_id)) unique_leads
            from
                yapocl_databox.insights_events_behavioral_fact_layer_365d
            where
                cast(date_parse(cast(year as varchar) || '-' ||
            cast(month as varchar) || '-' || cast(day as varchar),'%Y-%c-%e')
            as date) = date '{0}'
                    and event_type in ('Call','SMS','Send','Show') and lead_id != 'unknown'
            group by 1,2,3,4,5
            order by 1,2,3,4,5
            ) a
        group by 1,2,3,4,5
        order by 1,2,3,4,5
               """.format(self.params.get_date_from())
        return command

    def query_dau_platform(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    select
                        timedate,
                        sum(case when platform in ('Web')
                        then active_users end) dau_web,
                        sum(case when platform in ('MSite')
                        then active_users end) dau_msite,
                        sum(case when platform in ('AndroidApp','iOSApp')
                        then active_users end) dau_apps,
                        sum(case when platform = 'All Yapo' 
                        then active_users end) dau_all_yapo
                    from
                        dm_pulse.traffic_metrics
                    where
                        vertical = 'All Yapo'
                        and traffic_channel = 'All Yapo'
                        and main_category = 'All'
                        and timedate::date = '{0}'
                    group by 1
                    order by 1 """.format(self.params.get_date_from())

        return command

    def delete_traffic_metrics(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from """ + self.conf.db.table_traffic_metrics + """
                    where timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command

    def delete_unqiue_leads(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from """ + self.conf.db.table_unique_leads + """
                    where timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command


    def delete_buyers(self) -> str:
        """
        Method that return str with query
        """
        command = """
                    delete from """ + self.conf.db.table_buyers + """
                    where timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command


    def delete_dau_platform(self) -> str:
        """
        Method that return str with query
        """
        command = """
                    delete from """ + self.conf.db.table_dau_platform + """
                    where timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command
