import configparser
from datetime import datetime
import os
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging
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

def setup_aws_credentials():
    """Will fetch AWS_PROFILE or Access Key and Secret from Config File
    
    """
    log.debug("fetching AWS credentials")
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    if "AWS" in config and "PROFILE" in config["AWS"]:
        AWS_PROFILE = config.get('AWS', 'PROFILE')
        return get_credentials_from_profile(AWS_PROFILE)
    else:
        return (config['AWS_ACCESS_KEY_ID'], config['AWS_SECRET_ACCESS_KEY'])

def create_spark_session():
    """Creates a Spark Session with connectivity to S3
    
    """
    log.debug("getting SPARK session")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Will create songs and artists tables
    
    Positional arguments:
    spark -- (SparkSession) 
    input_data -- (string) S3 URL to Input data, no trailing backlash
    output_data -- (string) S3 URL to Output data, no trailing backlash 
    """
    # get filepath to song data file
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    
    # read song data file
    log.debug("Fetching songs data")
    df = spark.read.json(song_data)

    # extract columns to create songs table
    log.debug("Fetching songs table")
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates(["song_id"])
     
    # write songs table to parquet files partitioned by year and artist
    log.debug("Writing songs table to S3")
    songs_table.write.parquet(f'{output_data}/songs', mode='overwrite', partitionBy=['year', 'artist_id'])
    log.debug("Wrote songs table to S3")
    
    # extract columns to create artists table
    log.debug("Fetching artists table")    
    artists_table = df \
         .selectExpr('artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitude') \
         .dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    log.debug("Writing artists table to S3")
    artists_table.write.parquet(f'{output_data}/artists', mode='overwrite')
    log.debug("Wrote artists table to S3")

def process_log_data(spark, input_data, output_data):
    """Will create users, time, and songplays tables
    
    Positional arguments:
    spark -- (SparkSession) 
    input_data -- (string) S3 URL to Input data, no trailing backlash
    output_data -- (string) S3 URL to Output data, no trailing backlash 
    """
    # get filepath to log data file
    log_data = f'{input_data}/log_data/*/*/*.json'

    # read log data file
    log.debug("Fetching log data")
    df = spark.read.json(log_data)
    
    # extract columns for users table
    log.debug("Fetching users table")
    users_table = df \
         .selectExpr('userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level') \
         .dropDuplicates(["user_id"])
    
    log.debug("Writing users table to S3")
    users_table.write.parquet(f'{output_data}/users', mode='overwrite')
    log.debug("Wrote users table to S3")
        
    # extract columns to create time table
    log.debug("Fetching time table")
    time_table = df \
                   .dropDuplicates(['ts']) \
                   .withColumn("start_time", F.from_unixtime(F.col("ts") / 1000)) \
                   .withColumn('year', F.year('start_time')) \
                   .withColumn('month', F.month('start_time')) \
                   .withColumn('week', F.weekofyear('start_time')) \
                   .withColumn('weekday', F.dayofweek('start_time')) \
                   .withColumn('day', F.dayofmonth('start_time')) \
                   .withColumn('hour', F.hour('start_time')) \
                   .select("start_time", "year", "month", "week", "weekday", "day", "hour")
    
    
    # write time table to parquet files partitioned by year and month
    log.debug("Writing time table to S3")
    time_table.write.parquet(f'{output_data}/time', mode='overwrite', partitionBy=['year', 'month'])
    log.debug("Wrote time table to S3")

    # read in song data to use for songplays table
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    log.debug("Fetching songplays table")
    songplays_table = df \
              .join(song_df, (song_df.title == df.song) & (song_df.artist_name == df.artist)) \
              .join(time_table, F.from_unixtime(df.ts / 1000) == time_table.start_time) \
              .withColumn('time_year', time_table.year) \
              .withColumn('time_month', time_table.month) \
              .where(df.page == "NextSong") \
              .dropDuplicates(["start_time"]) \
              .selectExpr('start_time',
                          'time_year as year', 'time_month as month',
                          'userId as user_id', 
                          'level', 'song_id', 'artist_id', 'sessionId as session_id',
                          'location', 'userAgent as user_agent'
                         )

    # write songplays table to parquet files partitioned by year and month
    log.debug("Writing songplays table to S3")
    songplays_table.write.parquet(f'{output_data}/songplays', mode='overwrite', partitionBy=['year', 'month'])
    log.debug("Wrote songplays table to S3")


def main():
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY = setup_aws_credentials()
    spark = create_spark_session()
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    input_data = "s3a://udacity-dend"
    output_data = "s3a://claudiordgz-udacity-dend"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
