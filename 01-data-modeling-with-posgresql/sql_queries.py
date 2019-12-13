# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"
userlevel_table_drop = "DROP TABLE IF EXISTS userlevels;"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id int,
        start_time timestamp,
        user_id int,
        song_id char(18),
        artist_id char(18),
        session_id int,
        user_agent text,
        level varchar(100),
        location text
    );  
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id int,
        first_name varchar(100),
        last_name varchar(100),
        gender char(1),
        userlevel_id int
    );
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id char(18),
        title varchar(100),
        artist_id char(18),
        year int,
        duration numeric
    );
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id char(18),
        name varchar(100),
        location text,
        latitude numeric,
        longitude numeric
    );
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time timestamp,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    );
""")

userlevel_table_create = ("""
    CREATE TABLE userlevels(
        userlevel_id int,
        name varchar(100)
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays(songplay_id, start_time, user_id, song_id, artist_id, session_id, user_agent, level, location)
    VALUES (%d, %d, %s, %s, %s, %d, %s, %s, %s);
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, userlevel_id)
    VALUES (%d, %s, %s, %s, %d);
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %d, %f);
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %f, %f);
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
    VALUES (%d, %d, %d, %d, %d, %d, %d);
""")

userlevels_table_insert = ("""
    INSERT INTO userlevels(userlevel_id, name) 
    VALUES (%d, %s);
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    userlevel_table_create
]

drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
    userlevel_table_drop
]
