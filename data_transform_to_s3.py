import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd 

def album(data):
    album_list = []
    for i in data['items']:
        album_info = {
            'album_id': i['track']['album']['id'],
            'album_name': i['track']['album']['name'],
            'album_release_date': i['track']['album']['release_date'],
            'album_total_tracks': i['track']['album']['total_tracks'],
            'album_url': i['track']['album']['external_urls']['spotify']
        }
        album_list.append(album_info)
    return album_list

def song(data):
    top100_songs = []
    for i in data['items']:
        track_name = i['track']['name']
        track_id = i['track']['id']
        album_name = i['track']['album']['name']
        artist_name = i['track']['album']['artists'][0]['name']
        date_added = i['added_at']
        track_url = i['track']['external_urls']['spotify']
        track_duration = i['track']['duration_ms']
        album_id = i['track']['album']['id']
        artist_id = i['track']['album']['artists'][0]['id']
        top100_row = {
            'track_name': track_name,
            'track_id': track_id,
            'album_name': album_name,
            'artist_name': artist_name,
            'date_added': date_added,
            'track_url': track_url,
            'track_duration': track_duration,
            'album_id': album_id,
            'artist_id': artist_id
        }
        top100_songs.append(top100_row)
    return top100_songs

def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict = {
                        'artist_id': artist['id'],
                        'artist_name': artist['name'],
                        'external_url': artist['href']
                    }
                    artist_list.append(artist_dict)
    return artist_list            


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = "data-eng-etl"
    Key = "raw-data/to-processed/"
    
    spotify_data_list = []
    spotify_keys = []
    
    response = s3.list_objects(Bucket=Bucket, Prefix=Key)
    for file in response['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data_list.append(jsonObject)
            spotify_keys.append(file_key)
    
    for data in spotify_data_list:
        album_list = album(data)
        song_list = song(data)
        artist_list = artist(data)
        
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])
        
        song_df = pd.DataFrame.from_dict(song_list)

        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        song_df['date_added'] = pd.to_datetime(song_df['date_added'])
        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'])
        
        song_key = "transformed-data/song-view/songs_transformed_" + str(datetime.now()) + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=song_key, Body=song_content)
        
        album_key = "transformed-data/album-view/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer=StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)
        
        artist_key = "transformed-data/artist-view/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)
        
    