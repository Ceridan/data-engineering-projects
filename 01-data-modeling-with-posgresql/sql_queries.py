# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id bigserial PRIMARY KEY,
        start_time timestamp,
        user_id int,
        level varchar(100),
        song_id char(18),
        artist_id char(18),
        session_id int,
        location text,
        user_agent text
    );  
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id int PRIMARY KEY,
        first_name varchar(100),
        last_name varchar(100),
        gender char(1),
        level varchar(100) NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id char(18) PRIMARY KEY,
        title varchar(100) NOT NULL,
        artist_id char(18),
        year int,
        duration numeric
    );
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id char(18) PRIMARY KEY,
        name varchar(100) NOT NULL,
        location text,
        latitude numeric,
        longitude numeric
    );
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time timestamp PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

# In case of several attempts to insert data to the users table, they should be considered as an update operation.
# The user can change name and even gender, and what is most important for our business - he or she can switch from
# free level to paid level and vice versa.
user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE
    SET first_name = excluded.first_name
        , last_name = excluded.last_name
        , gender = excluded.gender
        , level = excluded.level;
""")

# The information about song maybe incomplete on the first insert. We want to update missing fields
# if we could find information about current  song from more sources.
song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
     ON CONFLICT (song_id) DO UPDATE
     SET artist_id = COALESCE(excluded.artist_id, songs.artist_id)
        , year = COALESCE(excluded.year, songs.year)
        , duration = COALESCE(excluded.duration, songs.duration);
""")

# Same as with songs we want to update missing information
artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO UPDATE
    SET location = COALESCE(excluded.location, artists.location)
        , latitude = COALESCE(excluded.latitude, artists.latitude)
        , longitude = COALESCE(excluded.longitude, artists.longitude);
""")

# We do not want to update time table, because the infromation will be the same for same start_time.
# Even if we lost some data in non-index fields we will easier recover it with single update using PRIMARY KEY data.
time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT s.song_id, s.artist_id
    FROM songs s
    INNER JOIN artists a ON a.artist_id = s.artist_id
    WHERE s.title = %s
        AND a.name = %s
        AND s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
