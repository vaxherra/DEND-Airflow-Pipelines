class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    
    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong' AND userid NOT IN (SELECT DISTINCT userid from {} ) 
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE staging_songs.song_id NOT IN (SELECT DISTINCT p.songid FROM {} as p )
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE staging_songs.artist_id NOT IN (SELECT DISTINCT p.artistid from {} as p )
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
        
        WHERE songplays.start_time NOT IN (SELECT DISTINCT p.start_time FROM {} as p )
        
    """)
    
    
    
### SQL Tests queries
    
    songplays_check_nulls = ("""
            SELECT COUNT(*)
            FROM songplays
            WHERE   playid IS NULL OR
                    start_time IS NULL OR
                    userid IS NULL;
        """)

    users_check_nulls = ("""
            SELECT COUNT(*)
            FROM users
            WHERE userid IS NULL;
        """)

    songs_check_nulls = ("""
            SELECT COUNT(*)
            FROM songs
            WHERE songid IS NULL;
        """)

    artists_check_nulls = ("""
            SELECT COUNT(*)
            FROM artists
            WHERE artistid IS NULL;
        """)

    time_check_nulls = ("""
            SELECT COUNT(*)
            FROM time
            WHERE start_time IS NULL;
        """)