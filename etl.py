import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.types import *

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """Creates the spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """Loads song data from S3 and writes tables back to S3"""
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    #create schema
    schema = StructType([
        StructField('num_songs', IntegerType(), True),
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', StringType(), True),
        StructField('artist_longitude', IntegerType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_name', StringType(), True),
        StructField('title', StringType(), True),
        StructField('duration', FloatType(), True),
        StructField('year', IntegerType(), True)
    ])
    
    # read song data file
    df = spark.read.json(song_data, multiLine=True, schema=schema)

    # extract columns to create songs table
    songs_table = df[['song_id','title', 'artist_id', 'year', 'duration']]    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
                     .mode('overwrite') \
                     .parquet(output_data + 'songs.parquet')

    # extract columns to create artists table
    artists_table = df[['artist_id', 'artist_name', 'artist_location', 
                        'artist_latitude', 'artist_longitude']]    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists.parquet')

def process_log_data(spark, input_data, output_data):
    """Loads log data from S3 and writes tables back to S3"""
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    #create schema
    schema = StructType([
        StructField('num_songs', IntegerType(), True),
        StructField('artist_id', StringType(), True),
        StructField('artist_latitude', StringType(), True),
        StructField('artist_longitude', IntegerType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_name', StringType(), True),
        StructField('title', StringType(), True),
        StructField('duration', FloatType(), True),
        StructField('year', IntegerType(), True)
    ])

    # read log data file
    df = spark.read.json(log_data, schema=schema)
    
    # filter by actions for song plays
    df = df.filter(df['page']=="NextSong")

    # extract columns for users table    
    users_table = df.selectExpr("userId AS user_id",
                                "firstName AS first_name", 
                                "lastName AS last_name", 
                                "gender", 
                                "level")
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users.parquet')

    # create timestamp column from original timestamp column
    def format_datetime(ts):
        """Formats a timestamp to a datetime"""
        return datetime.fromtimestamp(ts/1000.0)

    get_timestamp = udf(lambda x: format_datetime(int(x)),TimestampType())

    df = df.withColumn('timestamp', get_timestamp(col('ts')))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: format_datetime(int(x)),DateType())
    df = df.withColumn('datetime', get_timestamp(col('ts')))
    
    # extract columns to create time table
    time_table = df.select('timestamp',
                            hour(df.datetime).alias('hour'),
                            dayofmonth(df.datetime).alias('day'),
                            weekofyear(df.datetime).alias('week'),
                            month(df.datetime).alias('month'),
                            year(df.datetime).alias('year'),
                            dayofweek(df.datetime).alias('weekday')
                       )
    time_table = time_table.dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').parquet(output_data + 'time.parquet')

    # read in song data to use for songplays table
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    songs_df = spark.read.json(song_data, multiLine=True) 
    #join logs and songs tables
    joined_df = df.join(songs_df, (df.artist_name == songs_df.artist) &
                      (df.title == songs_df.song) &
                      (df.duration == songs_df.length)
                      ,"inner")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = joined_df.selectExpr('datetime AS start_time',
                                           'userId AS user_id',
                                           'level',
                                           'song_id',
                                           'artist_id',
                                           'sessionId AS session_id',
                                           'location',
                                           'userAgent AS user_agent',
                                           'MONTH(timestamp) AS month',
                                           'YEAR(timestamp) AS year') 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
                         .mode('overwrite') \
                         .parquet(output_data + 'songplays.parquet')

def main():
    """Iniaties spark and executes the processing of S3 JSON data"""
    spark = create_spark_session()
    input_data = 's3://udacity-dend/'
    output_data = 's3://jdsawsudacitybucket/data-lakes/parquet/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
