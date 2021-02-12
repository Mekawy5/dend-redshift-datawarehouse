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

# CREATE TABLES

staging_events_table_create= ("""
    create table if not exists staging_events
    (
        artist          VARCHAR
        auth            VARCHAR
        firstName       VARCHAR
        gender          CHAR(1)
        itemInSession   INT
        lastName        VARCHAR
        length          FLOAT
        level           VARCHAR
        location        VARCHAR
        method          VARCHAR
        page            VARCHAR
        registration    BIGINT
        sessionId       INT
        song            VARCHAR
        status          INT
        ts              TIMESTAMP
        userAgent       VARCHAR
        userId          INT
    );
""")

staging_songs_table_create = ("""
    create table if not exists staging_songs
    (
        num_songs           INT
        artist_id           VARCHAR
        artist_latitude     FLOAT
        artist_longitude    FLOAT
        artist_location     VARCHAR
        artist_name         VARCHAR
        song_id             VARCHAR
        title               VARCHAR
        duration            FLOAT
        year                INT
    );
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
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

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
