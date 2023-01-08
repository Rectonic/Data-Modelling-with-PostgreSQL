import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from io import StringIO

def bulk_copy(data):
    """function to bulk copy rows in a dataframe into a table
        we create a temporary buffer to store our dataframe into and then 
        transfer it using built-in copy_expert function

    Args:
        data (dataframe): dataframe to process
        query: sql query to execute using this function
    Returns: buffer with dataframe in CSV format in memory
    """
    buffer = StringIO()
    data.to_csv(buffer, index = False)
    buffer.seek(0)
    return buffer

def process_song_file(cur, conn, filepath):
    """processes all files from song_data library and stores all values into respective database tables

    Args:
        cur (_type_): cursor instance
        conn (_type_): connection instance
        filepath (string): path to the files
    """
    # open song file
    
    song_df = pd.DataFrame(columns=['song_id', 'title', 'artist_id', 'year', 'duration'])
    artist_df = pd.DataFrame(columns=['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    # get total number of files found
    num_files = len(process_data(filepath))
    print('{} files found in {}'.format(num_files, filepath))

    # insert song record
    
    for i, datafile in enumerate(process_data(filepath), 1):
        df = pd.read_json(datafile,lines = True)
        song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
        song_df = pd.concat([song_df, song_data])
        artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
        artist_df = pd.concat([artist_df, artist_data])
        print('{}/{} files processed.'.format(i, num_files))
    
    # insert artist record
    song_df.drop_duplicates(subset = 'song_id', ignore_index = True, inplace = True)   
    artist_df.drop_duplicates(subset = 'artist_id', ignore_index = True, inplace = True)
    cur.copy_expert(song_table_copy, file = bulk_copy(song_df))
    cur.copy_expert(artist_table_copy, file = bulk_copy(artist_df))
    conn.commit()


def process_log_file(cur, conn, filepath):
    """function to process data in log directory and store them into respective database tables

    Args:
        cur (_type_): cursor instance
        conn (_type_): connection instance
        filepath (string): path to the files
    """
    # open log file
    time_df = pd.DataFrame(columns= ['time', 'hour', 'day', 'week of year', 'month', 'year', 'weekday'])
    user_df = pd.DataFrame(columns= ['userId', 'firstName', 'lastName', 'gender', 'level'])
    
    num_files = len(process_data(filepath))
    print('{} files found in {}'.format(num_files, filepath))
    
    songplay_df_full = pd.DataFrame(columns=['ts', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent'])
    for i, datafile in enumerate(process_data(filepath), 1):
    
        df = pd.read_json(datafile, lines = True)

        # filter by NextSong action
        df = df[df['page'] == 'NextSong']

    
    
        # convert timestamp column to datetime
        df['ts'] = pd.to_datetime(df['ts'], unit = 'ms').apply(lambda x: x.round(freq = 'S'))
        t = pd.to_datetime(df['ts'], unit = 'ms')
    
        # insert time data records
        time_dt = [t.dt.time, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
        column_labels = ['time', 'hour', 'day', 'week of year', 'month', 'year', 'weekday']
        time_data = pd.DataFrame(dict(zip(column_labels,time_dt)))
        time_df = pd.concat([time_df,time_data])
        
        # load user table
        user_data = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
        user_df = pd.concat([user_data, user_df], ignore_index= True)
        user_df = user_df[user_df['userId'] != ''].dropna()
        user_df['userId'] = user_df['userId'].astype(int)
        # insert user records

        songplay_df = pd.DataFrame(columns=['ts', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent'])
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
        
            songplay_data = pd.DataFrame([{'ts': row.ts,'user_id': row.userId, 'level': row.level, 'song_id': songid, 'artist_id': artistid, 'session_id': row.sessionId, 'location': row.location, 'user_agent':row.userAgent}])
            songplay_df = pd.concat([songplay_df,songplay_data])
        print('{}/{} files processed.'.format(i, num_files))        
        songplay_df_full = pd.concat([songplay_df_full, songplay_df])

    user_df.drop_duplicates(subset='userId', keep= 'last', inplace=True)
    user_df.to_csv('./user.csv',index=False)
    time_df.drop_duplicates(subset = 'time', ignore_index = True, inplace = True)
    cur.copy_expert(time_table_copy, file = bulk_copy(time_df))
    cur.copy_expert(user_table_copy, file = bulk_copy(user_df))
    cur.copy_expert(songplay_table_copy, file = bulk_copy(songplay_df_full))
    conn.commit()
def process_data(filepath):
    """function to walkthrough directories to find paths to files with data

    Args:
        filepath (string): path to the files

    Returns:
        List: List with paths to JSON files 
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))


    return all_files
    # iterate over files and process



def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_song_file(cur, conn, filepath='data/song_data')
    process_log_file(cur, conn, filepath='data/log_data')

    conn.close()


if __name__ == "__main__":
    main()