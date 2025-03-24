import json
import base64
from google.cloud import pubsub_v1
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

def fetch_spotify_data(event, context = None):
    # Set up Spotify API credentials
    client_id = '2b4e50e2ff5d4417b3ead86dabfff855'  # Replace with your actual client ID
    client_secret = '05e072ce147d48318a0c7bc74afa694b'  # Replace with your actual client secret
    
    # Authenticate using client credentials
    credentials = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=credentials)
    
    # Extract the Spotify URL from the event data
    spotify_url = ''
    try:
        spotify_url = json.loads(event.data).get("playlist",'https://api.spotify.com/v1/albums/37i9dQZEVXbNG2KDcFcKOF')
    except:
        print(f'Error in extracting url from data: {event.data}')
        return
    
    if not spotify_url:
        print("No Spotify URL provided.")
        return

    # Use the full URL directly with Spotipy
    try:
        results = sp.track(spotify_url)  # Try to fetch track data
    except spotipy.exceptions.SpotifyException:
        try:
            results = sp.album(spotify_url)  # Try to fetch album data
        except spotipy.exceptions.SpotifyException:
            try:
                results = sp.artist(spotify_url)  # Try to fetch artist data
            except spotipy.exceptions.SpotifyException as e:
                print(f"Failed to fetch data for URL: {spotify_url}. Error: {e}")
                return

    # Publish the data to Pub/Sub
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('orbital-bee-454310-a5', 'spotify-etl')  # Replace with your project ID and topic

    # Convert data to JSON string and publish
    publisher.publish(topic_path, json.dumps(results, default=str).encode('utf-8'))
    print("Data published to Pub/Sub successfully.")
    return '{"status":"200", "data": "OK"}'