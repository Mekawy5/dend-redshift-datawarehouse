import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplay_table;"
user_table_drop = "drop table if exists user_table;"
song_table_drop = "drop table if exists song_table;"
artist_table_drop = "drop table if exists artist_table;"
time_table_drop = "drop table if exists time_table;"

# CREATE TABLES QUERIES

# events, songs staging tables, will be used to populate fact and dimension tables
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          CHAR(1),
        itemInSession   INT,
        lastName        VARCHAR,
        length          FLOAT,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    BIGINT,
        sessionId       INT,
        song            VARCHAR,
        status          INT,
        ts              TIMESTAMP,
        userAgent       VARCHAR,
        userId          INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs           INT,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INT
    );
""")

# fact table, distributed by song_id 
# http://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html#identity-clause
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id     INT         NOT NULL    IDENTITY(1,1)   PRIMARY KEY,
        start_time      TIMESTAMP   NOT NULL,
        user_id         INT         NOT NULL    SORTKEY,
        level           VARCHAR     NOT NULL,
        song_id         VARCHAR     NOT NULL    DISTKEY,
        artist_id       VARCHAR     NOT NULL,
        session_id      INT,
        location        VARCHAR,
        user_agent      VARCHAR,
        FOREIGN KEY(start_time) REFERENCES time(start_time),
        FOREIGN KEY(user_id)    REFERENCES users(user_id),
        FOREIGN KEY(song_id)    REFERENCES songs(song_id),
        FOREIGN KEY(artist_id)  REFERENCES artists(artist_id)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id         INT         NOT NULL    PRIMARY KEY,
        first_name      VARCHAR     SORTKEY,
        last_name       VARCHAR,
        gender          CHAR(1),
        level           VARCHAR     NOT NULL
    ) DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id     VARCHAR     NOT NULL    PRIMARY KEY     DISTKEY,
        title       VARCHAR     NOT NULL,
        artist_id   INT         NOT NULL    SORTKEY,
        year        INT,
        duration    FLOAT,
        FOREIGN KEY(artist_id)  REFERENCES artists(artist_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id   VARCHAR    NOT NULL     PRIMARY KEY,
        name        VARCHAR    NOT NULL     SORTKEY,
        location    VARCHAR,
        latitude    FLOAT,
        longitude   FLOAT
    ) DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time  TIMESTAMP   PRIMARY KEY   SORTKEY,
        hour        INTEGER     NOT NULL,
        day         INTEGER     NOT NULL,
        week        INTEGER     NOT NULL,
        month       INTEGER     NOT NULL,
        year        INTEGER     NOT NULL,
        weekday     INTEGER     NOT NULL
    ) DISTSTYLE ALL;
""")

# STAGING TABLES
# see format option docs
# https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-format.html#copy-format

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    region 'us-west-2'
    format as json {};
""").format(
    config.get('S3','LOG_DATA'),
    config.get('IAM_ROLE','ARN'),
    config.get('S3','LOG_JSONPATH')
)

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    region 'us-west-2'
    format as json 'auto';
""").format(
    config.get('S3','SONG_DATA'),
    config.get('IAM_ROLE','ARN')
)

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT DISTINCT e.ts,
        e.userId,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_events e 
    JOIN staging_songs s
    ON e.song = s.title
    AND e.artist = s.artist_name
    WHERE page = 'NextSong'
    AND e.song IS NOT NULL
    AND e.artist IS NOT NULL;
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id, first_name, last_name, gender, level
    )
    SELECT DISTINCT userId,
        firstName,
        lastName,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
    AND userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
    FROM staging_songs;
""")

# https://docs.aws.amazon.com/redshift/latest/dg/r_EXTRACT_function.html
time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        ts,
        EXTRACT(hour FROM ts),
        EXTRACT(day FROM ts),
        EXTRACT(week FROM ts),
        EXTRACT(month FROM ts),
        EXTRACT(year FROM ts),
        EXTRACT(dow FROM ts)
    FROM staging_events
    WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
