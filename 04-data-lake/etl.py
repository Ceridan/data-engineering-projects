import configparser
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, asc, desc
from pyspark.sql.functions import date_format, row_number, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('S3', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('S3', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Create session on the AWS EMR Spark cluster. Required to processing data using Spark"""

    spark = SparkSession \
        .builder \
        .appName('Sparkify Data Lake') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process raw songs dataset using Spark and create Songs and Artists dimensional tables stored in S3"""

    print('Start processing song data...')

    # Read song data file
    song_data_path = input_data + 'song_data/*/*/*/*'
    df = spark.read.json(song_data_path)

    # Process Data Frame with raw songs data and create Songs dimensional table stored in S3
    process_songs(spark, df, output_data)

    # Process Data Frame with raw songs data and create Artists dimensional table stored in S3
    process_artists(spark, df, output_data)

    print('Finish processing song data.')


def process_log_data(spark, input_data, output_data):
    """
        1. Process raw logs dataset using Spark and create Users and Time dimensional tables stored in S3.
        2. Process both raw logs and songs dataset and create Songplays fact table stored in S3.
    """

    print('Start processing log data...')

    # Read log data file
    log_data_path = input_data + 'log_data/*'
    log_df = spark.read.json(log_data_path)

    # Process Data Frame with raw logs data and create Users dimensional table stored in S3
    process_users(spark, log_df, output_data)

    # Process Data Frame with raw logs data and create Time dimensional table stored in S3
    process_time(spark, log_df, output_data)

    # Read song data file
    song_data_path = input_data + 'song_data/*/*/*/*'
    song_df = spark.read.json(song_data_path)

    # Process both Data Frames with raw logs and songs data and create Songplays fact table stored in S3
    process_songplays(spark, song_df, log_df, output_data)

    print('Finish processing log data.')


def process_songs(spark, df, output_data):
    """Process Data Frame with raw songs data using Spark and create Songs dimensional table stored in S3"""

    print('Processing songs...')

    # Define schema for the Songs table. Schema also could be inferred implicitly
    # but defining it manually protects us from wrong type conversions
    songs_schema = StructType([
        StructField('song_id', StringType(), nullable=False),
        StructField('title', StringType(), nullable=False),
        StructField('artist_id', StringType(), nullable=True),
        StructField('year', LongType(), nullable=True),
        StructField('duration', DoubleType(), nullable=True)
    ])

    # Cleanup data. Remove rows with empty song_id or title and select required fields for Songs table.
    # We also use dropDuplicates by song_id here to avoid the same song row appears twice in the table.
    songs_rdd = df \
        .filter(col('song_id').isNotNull()) \
        .filter(col('title').isNotNull()) \
        .dropDuplicates(['song_id']) \
        .select('song_id', 'title', 'artist_id', 'year', 'duration') \
        .rdd

    # Create Songs table using clean data and schema.
    songs_table = spark.createDataFrame(songs_rdd, songs_schema)

    print('Writing songs_table data frame to parquet to S3')

    # Write Songs table to parquet files partitioned by year and artist to S3
    songs_table_path = output_data + 'tables/songs/songs.parquet'
    songs_table \
        .write \
        .partitionBy('year', 'artist_id') \
        .mode('overwrite') \
        .parquet(songs_table_path)

    print('Songs table has been created.')


def process_artists(spark, df, output_data):
    """Process Data Frame with raw songs data using Spark and create Artists dimensional table stored in S3"""

    print('Processing artists...')

    # Define schema for the Artists table. Schema also could be inferred implicitly
    # but defining it manually protects us from wrong type conversions
    artists_schema = StructType([
        StructField('artist_id', StringType(), nullable=False),
        StructField('name', StringType(), nullable=False),
        StructField('location', StringType(), nullable=True),
        StructField('latitude', DoubleType(), nullable=True),
        StructField('longitude', DoubleType(), nullable=True)
    ])

    # Cleanup data. Remove rows with empty artist_id or artist_name and select required fields for Artists table.
    # We also use dropDuplicates by artist_id here to avoid the same artist row appears twice in the table.
    artists_rdd = df \
        .filter(col('artist_id').isNotNull()) \
        .filter(col('artist_name').isNotNull()) \
        .dropDuplicates(['artist_id']) \
        .select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
        .rdd

    # Create Artists table using clean data and schema.
    artists_table = spark.createDataFrame(artists_rdd, artists_schema)

    print('Writing artists_table data frame to parquet to S3')

    # Write Artists table to parquet files to S3
    artists_table_path = output_data + 'tables/artists/artists.parquet'
    artists_table \
        .write \
        .mode('overwrite') \
        .parquet(artists_table_path)

    print('Artists table has been created.')


def process_users(spark, df, output_data):
    """
        Process Data Frame with raw logs data using Spark and create Users dimensional table stored in S3.

        To process Users data properly we need to make two decisions:
        1.  Log file have different actions, ex. NextSong, Home, Login etc. Should we filter logs data by action or not?
            Because we want to store information about all of our users thus we do not want to filter data by action
            and we will write all users to the Users table even if them never perform NextSong action.

        2.  The same user can occurs multiple times in the log file. There are two approaches to deal with it:
            - We can create historical Users dimension table where each row will have extra fields
            EffectiveDateFrom and EffectiveDateTo. It allows us to analyze all changes that was made by the user,
            ex. he/she may change name, switch from free to paid subscription and vice versa.
            - Also we may store only the latest state of our users. It means that we will write to the Users dimension
            table only latest occurrence in the log file for each user (ordered by timestamp).
            For the current processing task we will use the second approach: write only the latest state of our users.
    """

    print('Processing users...')

    # Define schema for the Users table. Schema also could be inferred implicitly
    # but defining it manually protects us from wrong type conversions
    users_schema = StructType([
        StructField('user_id', LongType(), nullable=False),
        StructField('first_name', StringType(), nullable=True),
        StructField('last_name', StringType(), nullable=True),
        StructField('gender', StringType(), nullable=True),
        StructField('level', StringType(), nullable=True)
    ])

    # Use Window function to enumerate all occurrences of the single user in the log file.
    # When it is done, we just can select each row with the value 1 for the number of occurrences (also see next
    # code statement).
    users_window = Window \
        .partitionBy('userId') \
        .orderBy(col('ts').desc()) \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Cleanup data. Remove rows with empty userId, apply Window function to find latest occurrences for each user
    # and select required fields for Users table.
    # We also use dropDuplicates by userId here to avoid the same artist row appears twice in the table.
    # We also can avoid using dropDuplicates method here because final data already will be unique because of our logic
    # with Window function and getting only the latest row for each userId. But we add dropDuplicates here to make
    # our solution more robust.
    users_rdd = df \
        .filter(col('userId').isNotNull()) \
        .dropDuplicates('userId') \
        .withColumn('num', row_number().over(users_window)) \
        .withColumn('user_id', col('userId').cast(LongType())) \
        .filter(col('num') == 1) \
        .select('user_id', 'firstName', 'lastName', 'gender', 'level') \
        .rdd

    # Create Users table using clean data and schema.
    users_table = spark.createDataFrame(users_rdd, users_schema)

    print('Writing users_table data frame to parquet to S3')

    # Write Users table to parquet files to S3
    users_table_path = output_data + 'tables/users/users.parquet'
    users_table \
        .write \
        .mode('overwrite') \
        .parquet(users_table_path)

    print('Users table has been created.')


def process_time(spark, df, output_data):
    """
        Process Data Frame with raw logs data using Spark and create Time dimensional table stored in S3.

        To properly create Time table we need to convert timestamp field in the logs. There are two approaches
        how to deal with it:
        - Use Spark udf() function and write processing code as normal Python code.
        - Use power of Spark and its predefined functions to work with timestamp.
        For the current processing task we will use the second approach: rely on Spark predefined functions.
    """

    print('Processing time...')

    # Define schema for the Time table. Schema also could be inferred implicitly
    # but defining it manually protects us from wrong type conversions
    time_schema = StructType([
        StructField('start_time', TimestampType(), nullable=False),
        StructField('hour', IntegerType(), nullable=False),
        StructField('day', IntegerType(), nullable=False),
        StructField('week', IntegerType(), nullable=False),
        StructField('month', IntegerType(), nullable=False),
        StructField('year', IntegerType(), nullable=False),
        StructField('weekday', IntegerType(), nullable=False)
    ])

    # Take unique timestamps from the log data and apply various functions to extract different parts of datetime
    # on the select stage to get all required fields for the Time table.
    # We also use dropDuplicates by timestamp here to avoid the same timestamp row appears twice in the table.
    time_rdd = df \
        .select('ts') \
        .withColumn('timestamp', (col('ts') / 1000).cast(TimestampType())) \
        .dropDuplicates(['timestamp']) \
        .select(
            col('timestamp').alias('start_time'),
            hour('timestamp').alias('hour'),
            dayofmonth('timestamp').alias('day'),
            weekofyear('timestamp').alias('week'),
            month('timestamp').alias('month'),
            year('timestamp').alias('year'),
            date_format(col('timestamp'), 'F').cast(IntegerType()).alias('weekday')
        ) \
        .rdd

    # Create Time table using clean data and schema.
    time_table = spark.createDataFrame(time_rdd, time_schema)

    print('Writing time_table data frame to parquet to S3')

    # Write Time table to parquet files partitioned by year and month to S3
    time_table_path = output_data + 'tables/time/time.parquet'
    time_table \
        .write \
        .partitionBy('year', 'month') \
        .mode('overwrite') \
        .parquet(time_table_path)

    print('Time table has been created.')


def process_songplays(spark, song_df, log_df, output_data):
    """
        Process Data Frame with raw logs and songs data using Spark and create Songplays fact table stored in S3.

        To create Songplays table we need raw data from both logs and songs files. Here we will join both tables
        and the tricky part is to choose proper key for the joining. Joining also helps us to cleanup data, because
        we do not want to include rows to the Songplays table where logs data do not match songs data,
        ex. some song name appears in the log but it doesn't exist in the song data.
        Thus for current processing task we will choose joining by several conditions:
        - Songs data `title` should match logs data `song`.
        - Songs data `artist_name` should match logs data `artist`.
        - Songs data `duration` should match logs data `length`.
    """

    print('Processing songplays...')

    # Define schema for the Songplays table. Schema also could be inferred implicitly
    # but defining it manually protects us from wrong type conversions.
    # Songplays schema contains two additional columns: "year" and "month" for partitioning.
    songplays_schema = StructType([
        StructField('songplay_id', LongType(), nullable=False),
        StructField('start_time', TimestampType(), nullable=False),
        StructField('user_id', LongType(), nullable=False),
        StructField('level', StringType(), nullable=True),
        StructField('song_id', StringType(), nullable=False),
        StructField('artist_id', StringType(), nullable=False),
        StructField('session_id', LongType(), nullable=True),
        StructField('location', StringType(), nullable=True),
        StructField('user_agent', StringType(), nullable=True),
        StructField('year', IntegerType(), nullable=False),
        StructField('month', IntegerType(), nullable=False)
    ])

    # Cleanup data. Remove rows with empty song_id or artist_id from Songs data.
    clean_song_df = song_df \
        .filter(col('song_id').isNotNull()) \
        .filter(col('artist_id').isNotNull())

    # Cleanup data. Choose only NextSong actions from Log data.
    clean_log_df = log_df \
        .filter(col('page') == 'NextSong')

    # Join songs and logs data frames, enrich with missing columns and select required columns
    # to create Songplays table.
    # Also we use Spark function `monotonically_increasing_id` to create unique identifiers for Songplays table rows.
    songplays_rdd = clean_song_df \
        .join(clean_log_df,
              (clean_song_df.title == clean_log_df.song)
              & (clean_song_df.artist_name == clean_log_df.artist)
              & (clean_song_df.duration == clean_log_df.length)
              , 'inner') \
        .withColumn('id', monotonically_increasing_id() + 1) \
        .withColumn('start_time', (col('ts') / 1000).cast(TimestampType())) \
        .withColumn('user_id', col('userId').cast(LongType())) \
        .withColumn('year', year('start_time')) \
        .withColumn('month', month('start_time')) \
        .select('id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'sessionId', 'location',
                'userAgent', 'year', 'month') \
        .repartition('year', 'month') \
        .rdd

    # Create Songplays table using clean data and schema.
    songplays_table = spark.createDataFrame(songplays_rdd, songplays_schema)

    print('Writing songplays_table data frame to parquet to S3')

    # Write Songplays table to parquet files partitioned by year and month to S3
    songplays_table_path = output_data + 'tables/songplays/songplays.parquet'
    songplays_table \
        .write \
        .partitionBy('year', 'month') \
        .mode('overwrite') \
        .parquet(songplays_table_path)

    print('Songplays table has been created.')


def main():
    """Create Spark session and call functions to process raw logs and songs datasets"""

    # Create Spark session for application "Sparkify Data Lake"
    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = "s3n://ceri-sparkify"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    # Stops Spark session for the job
    spark.stop()


# Entrypoint for the Python program
if __name__ == "__main__":
    main()
