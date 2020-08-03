import configparser
import logging
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
log = logging.getLogger(__name__)

def get_credentials_from_profile(AWS_PROFILE):
    """Will use AWS_PROFILE to fetch a Session and retrieve Access Key and Secret Key
    
    Positional arguments:
    AWS_PROFILE -- (string) name of local AWS Profile
    """
    from boto3 import Session, setup_default_session

    setup_default_session(profile_name=AWS_PROFILE)

    session = Session()
    credentials = session.get_credentials()
    current_credentials = credentials.get_frozen_credentials()
    return (current_credentials.access_key, current_credentials.secret_key)

def setup_aws_credentials(cfg):
    """Will fetch AWS_PROFILE or Access Key and Secret from Config File
    
    """
    log.debug("fetching AWS credentials")
    config = configparser.ConfigParser()
    config.read(cfg)
    if "AWS" in config:
        if "PROFILE" in config["AWS"]:
            AWS_PROFILE = config.get('AWS', 'PROFILE')
            return get_credentials_from_profile(AWS_PROFILE)
        else:
            return (config["AWS"]['ACCESS_KEY_ID'], config["AWS"]['SECRET_ACCESS_KEY'])    
    else:
        return (config['AWS_ACCESS_KEY_ID'], config['AWS_SECRET_ACCESS_KEY'])


def create_spark_session(aws_settings, spark_config, app_name=""):
    """Creates a Spark Session with connectivity to S3
    
    """
    log.debug("getting SPARK session")
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY = setup_aws_credentials(aws_settings)
    sub_app_name = "" if not app_name else f": {app_name}"
    conf = SparkConf().setAppName(f"Capstone Project {sub_app_name}")
    packages = ["org.apache.hadoop:hadoop-aws:2.7.0"]
    for k, v in spark_config.items():
        if k == "spark.jars.packages" and v not in packages:
            packages.append(v)
        else:
            conf.set(k, v)
    conf.set("spark.jars.packages", ",".join(packages))
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    return spark
