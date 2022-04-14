from spConnector import spotifyConnector
from bqConnector import bqConnector
import pandas as pd
import json
import time

class dataScraper:

    def __init__(self):

        self.spConnector = spotifyConnector()
        self.bqWorker = bqConnector()

    def download_playlist_tracks(self):
        
        with open('playlists.json') as json_file:
            playlists_dict = json.load(json_file)
            playlists = playlists_dict['playlists']

        playlist_ids = []

        for item in playlists:
            playlist_id = item['ID']
            playlist_ids.append(playlist_id)

        all_playlist_tracks_df = pd.DataFrame()

        for p_id in playlist_ids:

            print("Pulling playlist: %s" % p_id)

            try:

                df = self.spConnector.get_playlist_tracks(p_id)
        
            except Exception as e:
                print("Error pulling playlist %s, error is %s" % p_id, e)

            all_playlist_tracks_df = pd.concat([all_playlist_tracks_df, df])

        return all_playlist_tracks_df

    def download_track_info(self, df):
        
        song_ids = df['songId'].to_list()

        results = self.spConnector.get_playlist_track_info(song_ids)

        return results

    def download_track_features(self, df):

        song_ids = df['songId'].to_list()

        results = self.spConnector.get_song_features(song_ids)

        return results

    def create_song_artist_library(self, df):

        artist_song_dict = {}

        song_ids = df['songId'].tolist()

        for song_id in song_ids:

            try:

                song_details = self.spConnector.get_track(song_id)

                artist_list = []

                for artist in song_details['artists']:
                    print('Pulled artist ID: ' + str(artist['id']))
                    artist_list.append(artist['id'])

                artist_song_dict[song_id] = artist_list
            
            except Exception as e:
                print(repr(e))

        artist_song_df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in artist_song_dict.items() ])).melt().dropna()
     
        return artist_song_df

    def get_artist_popularity(self, df):
        
        """
        Loops through the artist_ids in the list
        created above and requests the artist's info,
        then creates a DataFrame with some bits of information
        from that response.
        """
        artist_info_rows = []

        artist_id_list = df['artistId'].tolist()

        for artist_id in artist_id_list:

            try:

                print("Pulling artist: %s" % artist_id)

                row = self.spConnector.get_artist(artist_id)
                artist_info_rows.append(row)

            except Exception as e:
                print(repr(e))

        artist_info_df = pd.DataFrame(artist_info_rows, columns=['artistId', 'name', 'totalFollowers', 'popularity'])

        artist_info_df['date'] = pd.to_datetime('today').normalize()

        return artist_info_df

    def get_song_popularity(self, df):

        song_rows = []

        song_id_list = df['id'].tolist()

        for song_id in song_id_list:

            try:

                print("Pull song: %s" % song_id)

                row = self.spConnector.get_track_popularity(song_id)
                song_rows.append(row)
            
            except Exception as e:
                print(repr(e))

        song_info_df = pd.DataFrame(song_rows, columns=['songId', 'name', 'popularity'])

        song_info_df['date'] = pd.to_datetime('today').normalize()

        return song_info_df
