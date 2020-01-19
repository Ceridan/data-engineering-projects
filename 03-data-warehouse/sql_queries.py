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

# `song` and `artist` columns should be of type text because of very long names in case of bad character encoding.
staging_events_table_create = ("""
    CREATE TABLE stg.events(
        artist text,
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
        song text,
        status int,
        ts bigint,
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
        artist_name text,
        song_id char(18),
        title text,
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
        artist_id char(18),
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
        title text NOT NULL,
        artist_id char(18),
        year int,
        duration numeric
    )
    DISTSTYLE ALL;
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id char(18) NOT NULL SORTKEY,
        name text NOT NULL,
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
# Load songplays from staging tables. Using LEFT JOIN to join "stg.events" with "stg.songs"
# because it is possible situation when some song is missing in the metadata
# but we want to keep such events for further investigation.
songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time
        , user_id
        , level
        , song_id
        , artist_id
        , session_id
        , location
        , user_agent
    )
    SELECT TIMESTAMP 'epoch' + e.ts / 1000 * INTERVAL '1 Second' as start_time
        , e.userId
        , e.level
        , s.song_id
        , s.artist_id
        , e.sessionId
        , e.location
        , e.userAgent
    FROM stg.events e
    LEFT JOIN stg.songs s ON s.title = e.song
    WHERE e.Page = 'NextSong'
        AND s.song_id IS NOT NULL;
""")

# Load "users" is a bit tricky task because user can change first or/and last name
# and can change level (buy subscription). Thus we have to find latest entry
# for each user and insert it to the users table.
# More accurate way is to keep all changes in the "users" dimension table along with effective date
# for each row for the same userId but we omit it here for simplicity.
user_table_insert = ("""
    INSERT INTO users(
        user_id
        , first_name
        , last_name
        , gender
        , level
    )
    SELECT e.userId
        , e.firstName
        , e.lastName
        , e.gender
        , e.level
    FROM stg.events e
    INNER JOIN (
        SELECT userId
            , MAX(ts) as ts
        FROM stg.events
        WHERE userId IS NOT NULL
        GROUP BY userId
    ) ge ON ge.userId = e.userId AND ge.ts = e.ts;
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id
        , title
        , artist_id
        , year
        , duration
    )
    SELECT DISTINCT song_id
        , title
        , artist_id
        , year
        , duration
    FROM stg.songs
    WHERE song_id IS NOT NULL
        AND title IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id
        , name
        , location
        , latitude
        , longitude
    )
    SELECT DISTINCT artist_id
        , artist_name
        , artist_location
        , artist_latitude
        , artist_longitude
    FROM stg.songs
    WHERE artist_id IS NOT NULL
        AND artist_name IS NOT NULL;
""")

# Load all timestamps to the "time" dimension tables not only for the "NextSong" page events
# because the same dimension table could be used with other fact tables rather than only with "songplays".
time_table_insert = ("""
    INSERT INTO time(
        start_time
        , hour
        , day
        , week
        , month
        , year
        , weekday
    )
    SELECT DISTINCT TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 Second' as start_time
        , EXTRACT(hour FROM start_time) as hour
        , EXTRACT(day FROM start_time) as day
        , EXTRACT(week FROM start_time) as week
        , EXTRACT(month FROM start_time) as month
        , EXTRACT(year FROM start_time) as year
        , EXTRACT(weekday FROM start_time) as weekday
    FROM stg.events;
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
