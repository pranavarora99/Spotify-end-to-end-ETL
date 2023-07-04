import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlists = sp.user_playlists('spotify')
    
    Playlist_URL = "https://open.spotify.com/playlist/6UeSakyzhiEt4NB3UAd6NQ"
    playlist_URI = Playlist_URL.split("/")[-1].split("?")[0]
    
    data = sp.playlist_tracks(playlist_URI)
    
    client = boto3.client('s3')
    try:
        client.put_object(
            Bucket = "data-eng-etl",
            Key = "raw-data/to-processed/spotify_data.json",
            Body = json.dumps(data).encode('utf-8')
        )
    except Exception as e:
        print("Error:", str(e))
