import pandas as pd
import json
import os
import glob
import re
import psycopg2

from sql_queries import Queries
sql = Queries()

def single_quote_converter(sentence):
    sentence = re.sub(r'\'', r'<single_quote_tag>', sentence)
    return sentence

def process_song_data(cur, filepath, conn=None):
    del conn
    # open song data
    song_df = pd.read_json(filepath, lines=True)

    # insert artist records
    artist_columns = ['artist_id', "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artist_data = song_df.loc[0, artist_columns].values.tolist()
    query_artist = sql.artist_table_insert()

    cur.execute(query_artist, artist_data)

    # insert song record
    song_columns = ["song_id", "title", "artist_id", "year", "duration"]
    song_data = song_df.loc[0, song_columns].values.tolist()
    # change datatype from int64 to integer python
    song_data[3] = int(song_data[3])
    query_song = sql.song_table_insert()

    cur.execute(query_song, song_data)

def process_log_file(cur, filepath, conn=None):
    # Open log file
    log_df = pd.read_json(filepath, lines=True)

    # Filter data by 'NextSong' page
    log_df = log_df[log_df["page"] == 'NextSong']

    # Convert 'ts' timestamp to datetime
    log_df['ts'] = pd.to_datetime(log_df['ts'], unit='ms')

    # Insert datetime record
    ## Extract the timestamp, hour, day, week, month, year, weekday from 'ts' column
    time_columns = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_data = list(map(
        lambda x: [x.strftime('%Y-%m-%d %H:%M:%S'), x.hour, x.day, x.week, x.month, x.year, x.weekday()+1]
        ,log_df['ts'].copy()
    ))

    time_list_dict = []
    for time in time_data:
        time_list_dict.append(dict(zip(time_columns, time)))

    datetime_df = pd.DataFrame(time_list_dict)
    datetime_df.drop_duplicates()
    datetime_df.reset_index(drop=True)

    for _, row in datetime_df.iterrows():
        query_datetime = sql.datetime_table_insert()
        cur.execute(query_datetime, list(row))

    #insert user record
    user_columns = ["userId", "firstName", "lastName", "gender", "level"]
    user_data = log_df[user_columns].copy()
    user_data["gender"] = user_data["gender"].str.upper()  # upper gender
    user_data["userId"] = user_data["userId"].astype(str)  # convert Serial to String
    user_data = user_data.drop_duplicates(subset="userId", keep="last")
    user_data = user_data.reset_index(drop=True)

    for _, row in user_data.iterrows():
        query_user = sql.user_table_insert()
        cur.execute(query_user, list(row))

    #song_play table
    song_play_columns = ["ts", "userId", "level", "song", "artist", "length", "sessionId", "location", "userAgent"]
    song_play_df = log_df[song_play_columns].copy()

    ## Format timestamp tp be upload in the database
    song_play_df['ts'] = song_play_df['ts'].dt.strftime('%Y-%m-%d %H:%M:%S')

    ## replace double-quotes by single-quotes. Ex: "Des Ree" replaced by 'Des Ree'
        ## This step prepares for inserting dataframe song_select below
    song_play_df["song"] = song_play_df["song"].map(single_quote_converter, na_action='ignore')
    song_play_df["artist"] = song_play_df["artist"].map(single_quote_converter, na_action='ignore')

    ## create "index" attribute to control the order of records
    song_play_df["idx"] = range(1,len(song_play_df)+1)

    song_play_df.drop_duplicates()
    song_play_df.reset_index(drop=True, inplace=True)

    ## get song_id, artist_id from song table, artist table
    batch_size = 5_000
    columns_in = ["idx","song", "artist", "length"]
    columns_out = ["song", "artist"]
    ## inserting by batch helps whole dataset (song_select) to be loaded in batches of a specified size and transposed row to column.
    for index in range(0, song_play_df.shape[0], batch_size):
        query = sql.song_select(dataframe=song_play_df[columns_in].iloc[index:index + batch_size])
        temp_df = pd.read_sql(query, conn)
        song_play_df.loc[index:index + temp_df.shape[0], columns_out] = temp_df

    ## sort dataframe by "idx" column (The order is very important for mapping)
    song_play_df = song_play_df.sort_values(by='idx')

    song_play_df["song"] = song_play_df["song"].map(lambda value: value if value != 'None' else None)
    song_play_df["artist"] = song_play_df["artist"].map(lambda value: value if value != 'None' else None)
    song_play_df = song_play_df.drop(columns=['idx','length'])

    # Inser songplay records
    for _, row in song_play_df.iterrows():
        songplay_data = (row.ts, row.userId, row.level,
                         row.song, row.artist, row.sessionId, row.location,
                         row.userAgent)
        query_songplay = sql.song_play_table_insert()
        cur.execute(query_songplay, songplay_data)
        conn.commit()

def process_data(cur, conn, filepath, func):
    # get all files
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))  # retrieve pathnames to match with pattern *.json
        for f in files:
            all_files.append(os.path.abspath(f))  # combine all files into a list

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process (start at 1)
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile, conn)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

def main():
    conn = psycopg2.connect(
        user="admin",
        password="<postgresql>",
        host="localhost",
        port="5432",
        dbname="sparkifydb"
    )

    cur = conn.cursor()
    process_data(cur, conn, filepath="songs", func=process_song_data)
    process_data(cur, conn, filepath="event", func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()




