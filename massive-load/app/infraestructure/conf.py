import os
import environ


INI_PULSE = environ.secrets.INISecrets.from_path_in_env("APP_PULSE_SECRET")
INI_DB = environ.secrets.INISecrets.from_path_in_env("APP_DB_SECRET")
INI_DW = environ.secrets.INISecrets.from_path_in_env("APP_DW_SECRET")
INI_GBQ = environ.secrets.INISecrets.from_path_in_env("APP_GBQ_SECRET")


@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """

    @environ.config(prefix="PULSE")
    class AthenaConfig:
        """
        AthenaConfig class represeting the configuration to access
        pulse service
        """
        s3_bucket: str = INI_PULSE.secret(name="bucket",
                                          default=environ.var())
        user: str = INI_PULSE.secret(name="user",
                                     default=environ.var())
        access_key: str = INI_PULSE.secret(name="accesskey",
                                           default=environ.var())
        secret_key: str = INI_PULSE.secret(name="secretkey",
                                           default=environ.var())
        region: str = INI_PULSE.secret(name="region", default=environ.var())

    @environ.config(prefix="DB")
    class DB_BlocketConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        host: str = INI_DB.secret(name="host", default=environ.var())
        port: int = INI_DB.secret(name="port", default=environ.var())
        name: str = INI_DB.secret(name="dbname", default=environ.var())
        user: str = INI_DB.secret(name="user", default=environ.var())
        password: str = INI_DB.secret(name="password", default=environ.var())
        table: str = environ.var("dm_analysis.db_version")

    @environ.config(prefix="DW")
    class DB_DWConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        host: str = INI_DW.secret(name="host", default=environ.var())
        port: int = INI_DW.secret(name="port", default=environ.var())
        name: str = INI_DW.secret(name="dbname", default=environ.var())
        user: str = INI_DW.secret(name="user", default=environ.var())
        password: str = INI_DW.secret(name="password", default=environ.var())
        table: str = environ.var("ods.partners_leads")

    @environ.config(prefix="GBQ")
    class GBQConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        analytics_schema: str = INI_GBQ.secret(name="analytics_schema", default="analytics_279907210")
        type: str = INI_GBQ.secret(name="type", default="service_account")
        project_id: str = INI_GBQ.secret(name="project_id", default="yapo-dat-prd")
        private_key_id: str = INI_GBQ.secret(name="private_key_id", default=environ.var())
        private_key: str = INI_GBQ.secret(name="private_key", default=environ.var())
        client_email: str = INI_GBQ.secret(name="client_email", default=environ.var())
        client_id: str = INI_GBQ.secret(name="client_id", default=environ.var())
        auth_uri: str = INI_GBQ.secret(name="auth_uri", default=environ.var())
        token_uri: str = INI_GBQ.secret(name="token_uri", default=environ.var())
        auth_provider_x509_cert_url: str = INI_GBQ.secret(name="auth_provider_x509_cert_url", default=environ.var())
        client_x509_cert_url: str = INI_GBQ.secret(name="client_x509_cert_url", default=environ.var())

    @environ.config(prefix="PICKLES")
    class PicklesConfig:
        """
        Pickles files path to conversion process
        """
        pickles_path: str = os.environ.get("APP_PICKLES")


    athenaConf = environ.group(AthenaConfig)
    blocketConf = environ.group(DB_BlocketConfig)
    DWConf = environ.group(DB_DWConfig)
    PicklesConf = environ.group(PicklesConfig)
    GBQConf = environ.group(GBQConfig)


def getConf():
    return environ.to_config(AppConfig)
