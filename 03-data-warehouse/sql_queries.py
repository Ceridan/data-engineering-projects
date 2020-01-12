import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


# CREATE SCHEMAS

staging_schema_create = "CREATE SCHEMA IF NOT EXISTS stg;"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg.events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg.songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE stg.events(
        artist varchar(100),
        auth varchar(100),
        firstName varchar(100),
        gender char(1),
        itemInSession int,
        lastName varchar(100),
        length numeric,
        level varchar(100),
        location text,
        method varchar(10),
        page varchar(100),
        registration numeric,
        sessionId int,
        song varchar(100),
        status int,
        ts timestamp,
        userAgent text,
        userId int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE stg.songs(
        num_songs int,
        artist_id char(18),
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location text,
        artist_name varchar(100),
        song_id char(18),
        title varchar(100),
        duration numeric,
        year int
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id bigint IDENTITY(0, 1) SORTKEY,
        start_time timestamp NOT NULL,
        user_id int NOT NULL,
        level varchar(100) NOT NULL,
        song_id char(18) NOT NULL,
        artist_id char(18) NOT NULL,
        session_id int NOT NULL,
        location text,
        user_agent text
    )
    DISTSTYLE EVEN;
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id int NOT NULL SORTKEY,
        first_name varchar(100) NOT NULL,
        last_name varchar(100) NOT NULL,
        gender char(1),
        level varchar(100) NOT NULL
    )
    DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id char(18) NOT NULL SORTKEY,
        title varchar(100) NOT NULL,
        artist_id char(18),
        year int,
        duration numeric
    )
    DISTSTYLE ALL;
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id char(18) NOT NULL SORTKEY,
        name varchar(100) NOT NULL,
        location text,
        latitude numeric,
        longitude numeric
    )
    DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time timestamp NOT NULL SORTKEY,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekday int NOT NULL
    )
    DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY stg.events
    FROM {}
    IAM_ROLE {}
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY stg.songs
    FROM {}
    IAM_ROLE {}
    FORMAT AS JSON 'auto';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_schema_queries = [staging_schema_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
