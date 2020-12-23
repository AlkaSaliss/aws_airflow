class SqlQueries:
    """List of create, copy and insert SQL queries used by operators"""

    staging_events_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_events (
            artist VARCHAR,
            auth VARCHAR,
            firstName VARCHAR,
            gender VARCHAR,
            itemInSession INTEGER,
            lastName VARCHAR,
            length FLOAT,
            level VARCHAR,
            location VARCHAR,
            method VARCHAR,
            page VARCHAR,
            registration FLOAT,
            sessionId INTEGER,
            song VARCHAR,
            status INTEGER,
            ts TIMESTAMP,
            userAgent VARCHAR,
            userId INTEGER
        );
    """)

    staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs INTEGER,
            artist_id VARCHAR,
            artist_latitude FLOAT,
            artist_longitude FLOAT,
            artist_location VARCHAR,
            artist_name VARCHAR,
            song_id VARCHAR,
            title VARCHAR,
            duration FLOAT,
            year SMALLINT
        );
    """)

    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id BIGINT IDENTITY(0, 1) PRIMARY KEY,
            start_time TIMESTAMP REFERENCES time NOT NULL SORTKEY DISTKEY,
            user_id VARCHAR REFERENCES users NOT NULL,
            level VARCHAR,
            song_id VARCHAR REFERENCES songs NOT NULL,
            artist_id VARCHAR REFERENCES artists NOT NULL,
            session_id INTEGER,
            location VARCHAR,
            user_agent VARCHAR
        );
    """)

    user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR PRIMARY KEY SORTKEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            level VARCHAR
        );
    """)

    song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs (
            song_id VARCHAR PRIMARY KEY SORTKEY,
            title VARCHAR,
            artist_id VARCHAR REFERENCES artists NOT NULL,
            year SMALLINT,
            duration FLOAT
        );
    """)

    artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR PRIMARY KEY SORTKEY,
            name VARCHAR,
            location VARCHAR,
            latitude FLOAT,
            longitude FLOAT
        );
    """)

    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
            hour SMALLINT,
            day SMALLINT,
            week SMALLINT,
            month SMALLINT,
            year SMALLINT,
            weekday SMALLINT
        );
    """)

    songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
            SELECT
                events.ts, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
            FROM (SELECT *
                    FROM staging_events
                    WHERE page='NextSong') events
            JOIN staging_songs songs
                ON events.song = songs.title
                    AND events.artist = songs.artist_name
                    AND events.length = songs.duration
    """)

    song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
            SELECT DISTINCT song_id,
                title, 
                artist_id, 
                year,
                duration 
            FROM staging_songs
            WHERE song_id IS NOT NULL
    """)

    artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
            SELECT DISTINCT artist_id,
                artist_name, 
                artist_location, 
                artist_latitude, 
                artist_longitude 
            FROM staging_songs
            WHERE artist_id IS NOT NULL
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
            SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
                extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
            FROM songplays
    """)
    
    user_table_insert = ("""
        BEGIN TRANSACTION;
            DROP TABLE IF EXISTS users_stage;
            CREATE TEMP TABLE users_stage (LIKE users);

            INSERT INTO users_stage
                SELECT DISTINCT userId, 
                    firstName, 
                    lastName,
                    gender, 
                    level 
		        FROM staging_events
                WHERE userId IS NOT NULL AND page='NextSong';
            
            DELETE FROM users
            USING users_stage
            WHERE users.user_id = users_stage.user_id;

            INSERT INTO users 
                SELECT * FROM users_stage;
            
            DROP TABLE users_stage;
        END TRANSACTION;
    """)

