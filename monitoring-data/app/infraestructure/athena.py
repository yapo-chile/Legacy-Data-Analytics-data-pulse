import logging
from pyathena import connect
import pandas.io.sql as psql


class Athena:
    """
    Class that extract data from Athena database
    """
    def __init__(self, s3_bucket, user, aws_key, aws_secret, region):
        self.log = logging.getLogger('athena')
        date_format = """%(asctime)s,%(msecs)d %(levelname)-2s """
        info_format = """[%(filename)s:%(lineno)d] %(message)s"""
        log_format = date_format + info_format
        logging.basicConfig(format=log_format, level=logging.INFO)
        self.s3_bucket = s3_bucket
        self.user = user
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.region = region
        self.connection = None
        self.get_connection()

    def get_connection(self):
        """
        Method that get connection to S3 Bucket.
        """
        self.log.info('get_connection S3 %s%s', self.s3_bucket, self.user)
        s3_staging_dir = self.s3_bucket + self.user
        self.connection = connect(aws_access_key_id=self.aws_key,
                                  aws_secret_access_key=self.aws_secret,
                                  s3_staging_dir=s3_staging_dir,
                                  region_name=self.region)

    def get_data(self, query):
        """
        Method that returns pandas DataFrame from query to S3 Bucket
        """
        self.log.info('Run query : %s',
                      query.replace('\n', ' ').replace('\t', ' '))
        df_result = psql.read_sql(query, self.connection)
        return df_result

    def close_connection(self):
        """
        Method that close connection to S3 Bucket
        """
        self.log.info('Close Athena %s%s', self.s3_bucket, self.user)
        self.connection.close()
