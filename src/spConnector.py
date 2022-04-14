import json
import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
from utils import split_dataframe

class spotifyConnector:

    def __init__(self):
        
        with open('spotify-client-id.txt') as f:
            spotify_client_id = f.read()

        with open('spotify-client-secret.txt') as f:
            spotify_client_secret = f.read()

        client_credentials_manager = SpotifyClientCredentials(spotify_client_id, spotify_client_secret)
        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    def get_track(self, track_id):

        results = self.sp.track(track_id)

        return results

    def get_playlist_tracks(self, playlist_id):

        offset = 0

        all_responses = {}

        while True:

            response = self.sp.playlist_items(
                playlist_id,
                offset=offset
            )

            if len(response['items']) == 0:
                break

            all_responses.update(response)
            offset = offset + len(response['items'])

        all_rows = []

        all_items = all_responses['items']

        for item in all_items:

            try:
            
                updateStamp = datetime.now()
                playlist_id = playlist_id
                added_at = item['added_at']
                track = item['track']
                song_id = track['id']

                row = (updateStamp, added_at, playlist_id, song_id)

                all_rows.append(row)

            except Exception as e:
                print(repr(e))

        else:

            playlist_items_df = pd.DataFrame(all_rows, columns=['updateStamp', 'addedAt', 'playlistId', 'songId'])
            playlist_items_df['updateStamp'] = pd.to_datetime(playlist_items_df['updateStamp'])
            playlist_items_df['addedAt'] = pd.to_datetime(playlist_items_df['addedAt'])
            
            return playlist_items_df

    def get_playlist_track_info(self, tracks):

        song_meta={'id':[], 'album':[], 'name':[], 'artist_id':[],
            'artist':[]}

        for song_id in tracks:

            print("Pulling song: %s" % song_id)

            try:
            
                """
                Uses the track function to return
                meta information about a given song.
                """
                meta = self.get_track(song_id)

                # song id
                song_meta['id'].append(song_id)

                # album name
                album = meta['album']['name']
                song_meta['album']+=[album]

                # song name
                song = meta['name']
                song_meta['name']+=[song]

                # arists name
                s = ', '
                artist = s.join([singer_name['name'] for singer_name in meta['artists']])
                song_meta['artist']+=[artist]

                # artists uri
                s = ', '
                artist_id = s.join([art_id['id'] for art_id in meta['artists']])
                song_meta['artist_id']+=[artist_id]

            except Exception as e:
                print(repr(e))

        song_meta_df = pd.DataFrame.from_dict(song_meta)

        deduped_song_meta_df = song_meta_df.drop_duplicates(subset='id')

        return deduped_song_meta_df

    def get_song_features(self, song_ids):

        features_meta = {'song_id': [], 'danceability': [], 'energy': [], 'key': [],
            'loudness': [], 'mode': [], 'speechiness': [], 'acousticness' : [],
            'instrumentalness': [], 'liveness' : [], 'valence': [], 'tempo': [],
            'duration_ms': [], 'time_signature' : []}

        for song_id in song_ids:

            print("Pulling features for song: " +str(song_id))

            try:
            
                features_meta['song_id'] +=[song_id]

                feature_meta = self.sp.audio_features(song_id)

                # song danceability
                danceability = feature_meta[0]['danceability']
                features_meta['danceability']+=[danceability]

                # song energy
                energy = feature_meta[0]['energy']
                features_meta['energy']+=[energy]

                # song key
                key = feature_meta[0]['key']
                features_meta['key']+=[key]

                # song loudness
                loudness = feature_meta[0]['loudness']
                features_meta['loudness']+=[loudness]

                # song mode 
                mode = feature_meta[0]['mode']
                features_meta['mode']+=[mode]

                # song speechiness
                speechiness = feature_meta[0]['speechiness']
                features_meta['speechiness']+=[speechiness]

                # acousticness
                acousticness = feature_meta[0]['acousticness']
                features_meta['acousticness']+=[acousticness]

                # instrumentalness
                instrumentalness = feature_meta[0]['instrumentalness']
                features_meta['instrumentalness']+=[instrumentalness]

                # liveness
                liveness = feature_meta[0]['liveness']
                features_meta['liveness']+=[liveness]

                # valence
                valence = feature_meta[0]['valence']
                features_meta['valence']+=[valence]

                # tempo
                tempo = feature_meta[0]['tempo']
                features_meta['tempo']+=[tempo]

                #duration_ms
                duration_ms = feature_meta[0]['duration_ms']
                duration_ms = duration_ms/60000
                features_meta['duration_ms']+=[duration_ms]

                # time signature
                time_signature = feature_meta[0]['time_signature']
                features_meta['time_signature']+=[time_signature]

            except Exception as e:
            
                danceability = 0
                features_meta['danceability']+=[danceability]

                energy = 0 
                features_meta['energy']+=[energy]

                key = 0
                features_meta['key']+=[key]

                loudness = 0
                features_meta['loudness']+=[loudness]

                mode = 0
                features_meta['mode']+=[mode]

                speechiness = 0
                features_meta['speechiness']+=[speechiness]

                acousticness = 0
                features_meta['acousticness']+=[acousticness]

                instrumentalness = 0
                features_meta['instrumentalness']+=[instrumentalness]

                liveness = 0
                features_meta['liveness']+=[liveness]

                valence = 0 
                features_meta['valence']+=[valence]

                tempo = 0
                features_meta['tempo']+=[tempo]

                duration_ms = 0
                features_meta['duration_ms']+=[duration_ms]

                time_signature = 0
                features_meta['time_signature']+=[time_signature]

                print(repr(e))

        features_meta_df = pd.DataFrame.from_dict(features_meta)

        return features_meta_df

    def get_artist(self, artist_id):

        info = self.sp.artist(artist_id)
        
        artist_name = info['name']
        print("Pulled: " + str(artist_name))
        artist_followers = info['followers']['total']
        artist_popularity = info['popularity']

        row = [artist_id, artist_name, artist_followers, artist_popularity]

        return row

    def get_track_popularity(self, song_id):

        info = self.sp.track(song_id)

        print("Pulled: " + str(song_id))
        song_name = info['name']
        song_popularity = info['popularity']

        row = [song_id, song_name, song_popularity]

        return row
