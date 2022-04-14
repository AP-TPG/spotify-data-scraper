from dataScraper import dataScraper
from bqConnector import bqConnector
import pandas as pd

def main():

    scraper = dataScraper()
    bqWorker = bqConnector()

    existing_songs_df = bqWorker.download_existing_songs()

    """
    Pull all songs in every playlist - RUN DAILY.
    """
    playlist_tracks_df = scraper.download_playlist_tracks()
    bqWorker.upload_playlist_tracks(playlist_tracks_df)

    """
    Pull meta for pulled songs that are new to the library.
    """
    new_songs = playlist_tracks_df[~playlist_tracks_df.songId.isin(existing_songs_df.id)]

    new_tracks_detail = scraper.download_track_info(new_songs)
    bqWorker.upload_song_library(new_tracks_detail)

    """
    Pull song features for new songs
    """
    new_tracks_features = scraper.download_track_features(new_songs)
    bqWorker.upload_song_features(new_tracks_features)

    """
    To create a tabular library of unique songs and their artists
    which are both referenced by ID.
    """
    song_artist_library_df = scraper.create_song_artist_library(new_songs)
    artist_song_df = song_artist_library_df.rename(columns={"variable": "songId", "value": "artistId" })
    bqWorker.upload_song_artist_lookup(artist_song_df)

    """
    To get the daily popularity for all songs in the 
    song library.
    """
    song_daily_popularity_df = scraper.get_song_popularity(existing_songs_df)
    bqWorker.upload_song_popularity(song_daily_popularity_df)

    """
    To get the daily popularity and followers 
    for the artist library.
    """
    existing_artists = bqWorker.download_existing_artists()
    aritst_daily_popularity_df = scraper.get_artist_popularity(existing_artists)
    bqWorker.upload_artist_popularity(aritst_daily_popularity_df)
    
if __name__ == "__main__":
    main()