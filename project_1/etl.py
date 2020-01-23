import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *

def process_song_file(cur, filepath):
    """Opens filepath as JSON, transforms into input data and inserts to Songs and Artists Tables."""
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]]
    for _, row in song_data.iterrows():
        cur.execute(song_table_insert, row)
    
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    for _, row in artist_data.iterrows():
        cur.execute(artist_table_insert, row)

def timestamp_to_row(timestamp):
    """Receives timestamp as unix epoch in milliseconds and returns the input values for the Times table"""
    dt = pd.to_datetime(timestamp,unit='ms')
    return (dt, dt.hour, dt.day, dt.week, dt.month, dt.year, dt.weekday())


def process_log_file(cur, filepath):
    """Opens filepath as JSON, transforms into input data and inserts to Times, Users, and Songplays Tables."""
    # open log file
    df = pd.read_json(filepath, lines=True)
    df['userId'].replace('', np.nan, inplace=True)
    df = df[df.userId.notnull()]

    # filter by NextSong action
    df = df[df["page"] == "NextSong"] 

    # convert timestamp column to datetime
    time_data = list(df['ts'].apply(timestamp_to_row))
    column_labels = ('start_time','hour','day','week','month','year','weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)
    
    # insert time data records
    for _, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for _, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        # insert songplay record
        # start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
        ts = pd.to_datetime(row.ts, unit='ms')
        songplay_data = (ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()