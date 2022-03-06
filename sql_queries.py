#TODO 2: Create a class "Queries" in order to create and drop tables
"""Make sure install Postgresql program to create a connection between python and database.
We will CREATE TABLES (dimension and fact tables) and INSERT data INTO them.

Dimension tables: user, artist, song, calendar
# 'artist' tables: artist_id (PK), artist_name, location, artist_latitude, artist_longitude
# 'user' table: userId (PK), firstName, lastName, gender, level
# 'song' table: song_id(PK), title, artist_id, year, duration
# 'datetime' table: start_time (PK), hour, day, week, month, year, weekday.

FACT table
# 'song_play' table: create a 'songplayId' (PK), ts, userId, level, song_id, artist_id, sessionId, location, userAgent
"""
import re
class Queries():
    def __init__(self):
        """CREATE DIMENSION TABLES"""
        # artist_table
        artist_table_create = (
            "CREATE TABLE IF NOT EXISTS artist\n"
            "(artist_id VARCHAR PRIMARY KEY, name VARCHAR, location VARCHAR, latitude NUMERIC, longitude NUMERIC);\n"
        )
        # user_table
        user_table_create = (
            "CREATE TABLE IF NOT EXISTS users\n"
            "(user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender CHAR, level varchar);\n"
        )
        # song_table
        song_table_create = (
            "CREATE TABLE IF NOT EXISTS song\n"
            "(song_id VARCHAR PRIMARY KEY,title VARCHAR, artist_id VARCHAR, year SMALLINT, duration NUMERIC);\n"
        )
        # datetime_table
        datetime_table_create = (
            "CREATE TABLE IF NOT EXISTS datetime\n" 
            "(start_time TIMESTAMP PRIMARY KEY, hour int NOT NULL, day INT NOT NULL, week INT NOT NULL, month INT NOT NULL, year INT NOT NULL, weekday INT NOT NULL);\n"
        )

        """CREATE FACT TABLE"""
        # song_play_table
        song_play_table_create = (
            "CREATE TABLE IF NOT EXISTS song_play\n"
            "(songplay_id SERIAL PRIMARY KEY, start_time TIMESTAMP, user_id INT, level VARCHAR, song_id VARCHAR, artist_id VARCHAR, session_id INT, location VARCHAR, user_agent text);\n"
        )

        """DROP TABLES"""
        song_play_table_drop = "DROP TABLE IF EXISTS song_play;"
        user_table_drop = "DROP TABLE IF EXISTS users;"
        song_table_drop = "DROP TABLE IF EXISTS song;"
        artist_table_drop = "DROP TABLE IF EXISTS artist;"
        datetime_table_drop = "DROP TABLE IF EXISTS datetime;"

        # Query_list
        self.create_table_queries = [
            artist_table_create,
            user_table_create,
            song_table_create,
            datetime_table_create,
            song_play_table_create,
        ]
        self.drop_table_queries = [
            artist_table_drop,
            user_table_drop,
            song_table_drop,
            datetime_table_drop,
            song_play_table_drop,
        ]
    # With static method we don't need to access any properties of a class but makes sense that it belongs to the class
    # Example: Instead of def song_select(self, dataframe), we just def song_select(dataframe)
    # create a utility function as static method
    @staticmethod
    def artist_table_insert():
        query = (
            "INSERT INTO artist\n"
            "(artist_id, name, location, latitude, longitude)\n"
            "VALUES\n"
            "(%s, %s, %s, %s, %s)"
            "ON CONFLICT (artist_id)\n"
            "DO UPDATE SET\n"
            "name = EXCLUDED.name,\n"
            "location = EXCLUDED.location,\n"
            "latitude = EXCLUDED.latitude,\n"
            "longitude = EXCLUDED.longitude;\n"
        )
        return query

    @staticmethod
    def user_table_insert():
        query = (
            "INSERT INTO users\n"
            "(user_id, first_name, last_name, gender, level)\n"
            "VALUES\n"
            "(%s, %s, %s, %s, %s)"
            "ON CONFLICT (user_id)\n"
            "DO UPDATE SET\n"
            "first_name = EXCLUDED.first_name,\n"
            "last_name = EXCLUDED.last_name,\n"
            "gender = EXCLUDED.gender,\n"
            "level = EXCLUDED.level;\n"
        )
        return query

    @staticmethod
    def song_table_insert():
        query = (
            "INSERT INTO song\n"
            "(song_id, title, artist_id, year, duration)\n"
            "VALUES\n"
            "(%s, %s, %s, %s, %s)"
            "ON CONFLICT (song_id)\n"
            "DO UPDATE SET\n"
            "title = EXCLUDED.title,\n"
            "artist_id = EXCLUDED.artist_id,\n"
            "year = EXCLUDED.year,\n"
            "duration = EXCLUDED.duration;\n"
        )
        return query

    @staticmethod
    def datetime_table_insert():
        query = (
            "INSERT INTO datetime\n"
            "(start_time, hour, day, week, month, year, weekday)\n"
            "VALUES\n"
            "(%s, %s, %s, %s, %s, %s, %s)"
            "ON CONFLICT (start_time)\n"
            "DO UPDATE SET\n"
            "hour = EXCLUDED.hour,\n"
            "day = EXCLUDED.day,\n"
            "week = EXCLUDED.week,\n"
            "month = EXCLUDED.month,\n"
            "year = EXCLUDED.year,\n"
            "weekday = EXCLUDED.weekday;\n"
        )
        return query


    @staticmethod
    def song_play_table_insert():
        query = (
            "INSERT INTO song_play (\n"
            "    start_time,\n"
            "    user_id,\n"
            "    level,\n"
            "    song_id,\n"
            "    artist_id,\n"
            "    session_id,\n"
            "    location,\n"
            "    user_agent\n"
            ")\n"
            "VALUES\n"
            "(%s, %s, %s, %s, %s, %s, %s, %s)"
            ";\n"
        )
        return query

    @staticmethod
    def song_select(dataframe):
        dataframe = str(dataframe.values.tolist())[1:-1]
        dataframe = re.sub(r'\[', r'        (', dataframe)
        dataframe = re.sub(r'\]', r')', dataframe)
        dataframe = re.sub("<single_quote_tag>", "''", dataframe)
        dataframe += '\n'
        query = (
            "SELECT\n"
            "    song_artist.song_id song, --input_data.song\n"
            "    song_artist.artist_id artist --input_data.artist\n"
            "FROM (\n"
            "    SELECT index, song, artist, duration\n"
            "    FROM (\n"
            "        VALUES\n"
            f"{dataframe}"
            "    ) AS headers (index, song, artist, duration)\n"
            ") AS input_data\n"
            "LEFT JOIN (\n"
            "    SELECT\n"
            "        song.song_id,\n"
            "        song.title,\n"
            "        song.duration,\n"
            "        artist.artist_id,\n"
            "        artist.name\n"
            "    FROM song\n"
            "    LEFT JOIN artist\n"
            "    ON song.artist_id = artist.artist_id\n"
            ") song_artist\n"
            "ON input_data.song = song_artist.title\n"
            "AND input_data.artist = song_artist.name\n"
            "AND input_data.duration = song_artist.duration\n"
            "ORDER BY input_data.index ASC;\n"
        )
        return query







