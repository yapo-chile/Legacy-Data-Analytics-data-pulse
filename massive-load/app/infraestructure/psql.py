import logging
import psycopg2
import psycopg2.extras
import pandas as pd


class Database:
    """
    Class that allow do operations with postgresql database.
    """
    def __init__(self, conf) -> None:
        self.log = logging.getLogger('psql')
        date_format = """%(asctime)s,%(msecs)d %(levelname)-2s """
        info_format = """[%(filename)s:%(lineno)d] %(message)s"""
        log_format = date_format + info_format
        logging.basicConfig(format=log_format, level=logging.INFO)
        self.conf = conf
        self.connection = None
        self.get_connection()

    def database_conf(self) -> dict:
        """
        Method that return dict with database credentials.
        """
        return {"host": self.conf.host,
                "port": self.conf.port,
                "user": self.conf.user,
                "password": self.conf.password,
                "dbname": self.conf.name}

    def get_connection(self) -> None:
        """
        Method that returns database connection.
        """
        self.log.info('get_connection DB %s/%s', self.conf.host, self.conf.name)
        self.connection = psycopg2.connect(**self.database_conf())
        self.connection.set_client_encoding('UTF-8')

    def execute_command(self, command) -> None:
        """
        Method that allow execute sql commands such as DML commands.
        """
        self.log.info('execute_command : %s',
                      command.replace('\n', ' ').replace('\t', ' '))
        cursor = self.connection.cursor()
        cursor.execute(command)
        self.connection.commit()
        cursor.close()

    def select_to_dict(self, query) -> pd.DataFrame:
        """
        Method that from query transform raw data into dict.
        """
        self.log.info('Query : %s', query.replace(
            '\n', ' ').replace('    ', ' '))
        cursor = self.connection.cursor()
        cursor.execute(query)
        fieldnames = [name[0] for name in cursor.description]
        result = []
        for row in cursor.fetchall():
            rowset = []
            for field in zip(fieldnames, row):
                rowset.append(field)
            result.append(dict(rowset))
        pd_result = pd.DataFrame(result)
        cursor.close()
        return pd_result

    def insert_data(self, data_dict: pd.DataFrame) -> None:
        self.log.info('INSERT INTO %s', self.conf.table)
        page_size: int = 10000
        with self.connection.cursor() as cursor:
            psycopg2.extras \
                .execute_values(cursor,
                                """ INSERT INTO """ + self.conf.table +
                                """ ( timedate,
                                      vertical,
                                      category,
                                      integrador,
                                      codigo_inmo,
                                      patente,
                                      list_id,
                                      list_time,
                                      deletion_date,
                                      region,
                                      comuna,
                                      estate_type,
                                      squared_meters,
                                      rooms,
                                      car_year,
                                      brand,
                                      model,
                                      km,
                                      price,
                                      currency,
                                      number_of_views,
                                      number_of_show_phone,
                                      number_of_calls,
                                      number_of_ad_replies,
                                      sucursal,
                                      email
                                    )
                                    VALUES %s; """, ((
                                        row.timedate,
                                        row.vertical,
                                        row.category,
                                        row.integrador,
                                        row.codigo_inmo,
                                        row.patente,
                                        row.list_id,
                                        row.list_time,
                                        row.deletion_date,
                                        row.region,
                                        row.comuna,
                                        row.estate_type,
                                        row.squared_meters,
                                        row.rooms,
                                        row.car_year,
                                        row.brand,
                                        row.model,
                                        row.km,
                                        row.price,
                                        row.currency,
                                        row.number_of_views,
                                        row.number_of_show_phone,
                                        row.number_of_calls,
                                        row.number_of_ad_replies,
                                        row.sucursal,
                                        row.email
                                    ) for row in data_dict.itertuples()),
                                page_size=page_size)
            self.log.info('INSERT %s COMMIT.', self.conf.table)
            self.connection.commit()
            self.log.info('CLOSE CURSOR %s', self.conf.table)
            cursor.close()


    def close_connection(self):
        """
        Method that close connection to postgresql database.
        """
        self.log.info('Close connection DB : %s/%s',
                      self.conf.host,
                      self.conf.name)
        self.connection.close()
