import pandas as pd
import json
import os
import glob
import re
import psycopg2

from sql_queries import Queries
sql = Queries()

#TODO 1: Read multiple JSON files from a folder
"""Function to extract JSON files from folder"""
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json')) #retrieve pathnames to match with pattern *.json
        for f in files:
            all_files.append(os.path.abspath(f)) #combine all files into a list
    return all_files

song_files = get_files("songs") #attention the place where the folder is
#perform unit test to check the result
print(pd.read_json(song_files[0], lines=True).to_string())

""" Create a list of songs which contains all songs from all json files"""
"""song data - metadata"""
song_list = []
for filepath in song_files:
    with open(filepath) as f:
        song_list.append(json.load(f))

song_df = pd.DataFrame(song_list)

"""Log data """
event_files = get_files("event")
event_log_list = []
for path in event_files:
    event_df = pd.read_json(path, lines=True)
    event_log_list.append(event_df)

#perform unit test to check data and the number of files
a = pd.read_json(event_files[0], lines=True)
print(a.to_string())
print(f"Total number of JSON file: {len(event_log_list)}")

#TODO 2: Create a class "Queries" in order to create and drop tables (sql_queries.py)
#TODO 3: Create a database and connect to Postgresql (create_tables.py)

#TODO 4: ETL PROCESS - Extract data for creating tables
"""Process song_dataset: """
"""artist table"""
artist_columns = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
artist_data = song_df[artist_columns].copy()
artist_data = artist_data.drop_duplicates() #remove duplicated values
artist_data = artist_data.reset_index(drop=True) #set index for the data frame already created
print(f"Total number of artists:{artist_data.shape}") #df.shape() show number of rows and columns

# insert records into data frame
query_artist = sql.artist_table_insert()

"""song table"""
song_columns = ["song_id", "title", "artist_id", "year", "duration"]
song_data = song_df[song_columns].copy()
song_data = song_data.drop_duplicates()
song_data = song_data.reset_index(drop=True)
print(f"Total number of songs:{song_data.shape}")

##insert records into data frame
query_song = sql.song_table_insert()

"""Process log_data contains songplay data:
Songplay data is identified by filtering for actions initiated from 'NextSong' page. Therefore, we need to
- check all status in "page" column
- Filter dataframe by 'NextSong' page
"""
#check all status in page column. We take a log as an example
logpath = event_files[0]
log_df = pd.read_json(logpath, lines=True)
status_page = log_df["page"].value_counts()
#there're 3 status including NextSong, Home and Upgrade in page column.
print(status_page)

#Filter data by 'NextSong' page
filtered_log_df = log_df[log_df["page"] == 'NextSong']
print(f"Total number of logs with 'NextSong' data: {filtered_log_df.shape[0]}")

"""user table"""
user_columns = ["userId", "firstName", "lastName", "gender", "level"]
user_data = filtered_log_df[user_columns].copy()
user_data["gender"] = user_data["gender"].str.upper() # upper gender
user_data["userId"] = user_data["userId"].astype(str) # convert Serial to String

user_data = user_data.drop_duplicates(subset="userId", keep="last")
user_data = user_data.reset_index(drop=True)
print(f"Total number of users:{user_data.shape}")

#insert values into data frame
query_user = sql.user_table_insert()

"""datetime table"""
# data type of 'ts' columns is int64. We need to convert "ts" timestamp to datetime
print(filtered_log_df["ts"].dtypes)

filtered_log_df['ts'] = pd.to_datetime(filtered_log_df['ts'], unit='ms') #milliseconds

# Extract the timestamp, hour, day, week, month, year, weekday from 'ts' column
## we create a list of time by using lambda function (argument: expression)
time_columns = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
time_data = list(map(
    lambda x: [x.strftime('%Y-%m-%d %H:%M:%S'), x.hour, x.day, x.week, x.month, x.year, x.weekday()+1]
    ,filtered_log_df['ts'].copy()
))

# create a data frame
time_list_dict = []
for time in time_data:
    time_list_dict.append(dict(zip(time_columns, time)))

datetime_df = pd.DataFrame(time_list_dict)
datetime_df.drop_duplicates()
datetime_df.reset_index(drop=True)
print(datetime_df)
print(f"Total number of rows:{datetime_df.shape}")

#Insert values into datetime table
query_datetime = sql.datetime_table_insert()

"""FACT TABLE: song_play table """
song_play_columns = ["ts", "userId", "level", "song", "artist", "length", "sessionId", "location", "userAgent"]
song_play_df = filtered_log_df[song_play_columns].copy()

song_play_df['ts'] = song_play_df['ts'].dt.strftime('%Y-%m-%d %H:%M:%S')

# replace double-quotes by single-quotes. Ex: "Des Ree" replaced by 'Des Ree'
    ## This step prepares for inserting dataframe song_select below
def single_quote_converter(sentence):
    sentence = re.sub(r'\'', r'<single_quote_tag>', sentence)
    return sentence
song_play_df["song"] = song_play_df["song"].map(single_quote_converter, na_action='ignore')
song_play_df["artist"] = song_play_df["artist"].map(single_quote_converter, na_action='ignore')

# create "index" attribute to control the order of records
song_play_df["idx"] = range(1, len(song_play_df)+1)

song_play_df.drop_duplicates()
song_play_df.reset_index(drop=True, inplace=True)

# get song_id, artist_id from song table, artist table
batch_size = 5_000
columns_in = ["idx","song", "artist", "length"]
columns_out = ["song", "artist"]
## inserting by batch helps whole dataset (song_select) to be loaded in batches of a specified size and transposed row to column.
for index in range(0, song_play_df.shape[0], batch_size):
    query = sql.song_select(dataframe=song_play_df[columns_in].iloc[index:index + batch_size])
    """Before executing temp_df, song and artist tables must be created in database 'sparkifydb' """
    temp_df = pd.read_sql(query, psycopg2.connect(user="admin",
                                                password="<postgresql>",
                                                host="localhost",
                                                port="5432",
                                                dbname="sparkifydb"))
    song_play_df.loc[index:index + temp_df.shape[0], columns_out] = temp_df

## sort dataframe by "idx" column (The order is very important for mapping)
song_play_df = song_play_df.sort_values(by='idx')

song_play_df["song"] = song_play_df["song"].map(lambda value: value if value != 'None' else None)
song_play_df["artist"] = song_play_df["artist"].map(lambda value: value if value != 'None' else None)
song_play_df = song_play_df.drop(columns=['idx','length'])

print(song_play_df.to_string())
print(f"Total number of song_play rows:{song_play_df.shape}")

# insert songplay records
for _, row in song_play_df.iterrows():
    songplay_data = (row.ts, row.userId, row.level,
                     row.song, row.artist, row.sessionId, row.location,
                     row.userAgent)
    query_songplay = sql.song_play_table_insert()















