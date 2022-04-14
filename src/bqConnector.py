from google.cloud import bigquery
from google.oauth2 import service_account
import os

class bqConnector:

    def __init__(self):

        credentials = service_account.Credentials.from_service_account_file(
            'bigQuery-SA.json'
        )

        self.client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        self.project = self.client.project

    def upload_playlist_tracks(self, df):

        schema = [
            bigquery.SchemaField("updateStamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("addedAt", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("playlistId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("songId", "STRING", mode="NULLABLE"),   
        ]

        table_id = 'spotify_data.DAILY_PLAYLIST_LISTINGS'

        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_APPEND')

        job = self.client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )

        job.result()

    def upload_song_library(self, df):

        schema = [
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("album", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("artist_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("artist", "STRING", mode="NULLABLE")
        ]
        
        table_id = 'spotify_data.SONG_LIBRARY'

        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_APPEND')

        job = self.client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )

        job.result()

    def upload_song_features(self, df):

        schema = [
            bigquery.SchemaField("song_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("danceability", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("energy", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("key", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("loudness", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("mode", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("speechiness", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("acousticness", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("instrumentalness", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("liveness", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("valence", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("tempo", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("duration_ms", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("time_signature", "INTEGER", mode="NULLABLE")
        ]

        table_id = 'spotify_data.SONG_FEATURES'

        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_APPEND')

        job = self.client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )

        job.result()

    def upload_song_artist_lookup(self, df):

        schema = [
            bigquery.SchemaField("songId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("artistId", "STRING", mode="NULLABLE")
        ]

        table_id = 'spotify_data.SONG_ARTIST_LOOKUP'

        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_APPEND')

        job = self.client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )

        job.result()

    def upload_artist_popularity(self, df):

        schema = [
            bigquery.SchemaField('date', "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField('artistId', "STRING", mode="NULLABLE"),
            bigquery.SchemaField('name', "STRING", mode="NULLABLE"),
            bigquery.SchemaField('totalFollowers', "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField('popularity', "INTEGER", mode="NULLABLE"),
        ]

        table_id = 'spotify_data.DAILY_ARTIST_POPULARITY'

        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_APPEND')

        job = self.client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )

        job.result()

    def upload_song_popularity(self, df):

        schema = [
            bigquery.SchemaField('date', "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField('songId', "STRING", mode="NULLABLE"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("popularity", "INTEGER", mode="NULLABLE")  
        ]

        table_id = 'spotify_data.DAILY_SONG_POPULARITY'

        job_config = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_APPEND')

        job = self.client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )

        job.result()
    
    def download_existing_songs(self):

        sql = """SELECT distinct(id) from `dmr-stg.spotify_data.SONG_LIBRARY`"""

        df = self.client.query(sql).to_dataframe()

        return df

    def download_playlist_listings(self):

        sql = """SELECT * FROM `dmr-stg.spotify_data.DAILY_PLAYLIST_LISTINGS`"""

        df = self.client.query(sql).to_dataframe()

        return df

    def download_existing_artists(self):

        sql = """SELECT distinct(artistId) FROM `dmr-stg.spotify_data.SONG_ARTIST_LOOKUP`"""

        df = self.client.query(sql).to_dataframe()

        return df
