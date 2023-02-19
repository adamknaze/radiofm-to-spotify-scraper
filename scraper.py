import json
import sys
from datetime import datetime
import itertools
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.cache_handler import MemoryCacheHandler
from fuzzywuzzy import fuzz
import psycopg2 as pg

from radio_scrapers import scrape_range


def scrape_n_store(config, station='radiofm'):
    '''
    Scrape tracks from public radio playlist, insert to db.
    '''

    # connect DB
    conn = pg.connect(
        host=config['db_host'],
        database=config['db'],
        user=config['db_user'],
        password=config['passw']
    )

    # scrape tracks
    tracks_timestamps = scrape_range(station, config['stations'][station])

    # find on spofity & insert into db
    sp = spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=config['sp_client_id'],
            client_secret=config['sp_client_secret'],
            cache_handler=MemoryCacheHandler()      # used in Lambda
        ),
        requests_timeout=10
    )

    cur = conn.cursor()
    cur.execute(f"SELECT time FROM {config['stations'][station]['db_table']} ORDER BY time DESC NULLS LAST LIMIT 1")
    maxtime_db = cur.fetchone()
    maxtime_db = maxtime_db[0].replace(tzinfo=None) if maxtime_db is not None else datetime.min

    insert_data = []

    for track_timestamp in tracks_timestamps:
        if track_timestamp[2].replace(tzinfo=None) < maxtime_db:
            continue

        try:
            track_id = find_on_spotify(sp, track_timestamp[0], track_timestamp[1])
        except Exception as e:
            print(e) 
            track_id = None

        insert_data.append([
            str(track_timestamp[2]),
            track_timestamp[0].replace("'", "''"),
            track_timestamp[1].replace("'", "''"),
            track_id
        ])

    tick = "'"
    insert_strs = [f"('{x[0]}', '{x[1]}', '{x[2]}', {tick + x[3] + tick if x[3] is not None else 'NULL'})" for x in insert_data]
    val_str = ', '.join(insert_strs)

    sql = f"""
        INSERT INTO {config['stations'][station]['db_table']} (time, artist, song, spotify_id) VALUES {val_str}
        ON CONFLICT ON CONSTRAINT {config['stations'][station]['db_table']}_time_artist_song_key DO NOTHING;
    """
    cur.execute(sql)

    conn.commit()
    cur.close()
    conn.close()


def find_on_spotify(sp, artist, song):

    track_name_words = song.split()
    artist_name_words = artist.split()
    search_strings = []
    search_strings.append(song + ' ' + artist)
    search_strings.append(song)
    for i in range(len(track_name_words)):
        if (i > 4):
            break
        for j in range(len(artist_name_words)):
            if (j > 3):
                break
            search_strings.append(' '.join(track_name_words[:i + 1])+' '+' '.join(artist_name_words[:j]))

    results = []
    for string in search_strings:
        results.append(sp.search(string, type='track')['tracks']['items'])

    for item in itertools.chain.from_iterable(results):
        if (fuzz.token_set_ratio(song, item['name']) > 75):
            for found_artist in item['artists']:
                if (fuzz.token_set_ratio(artist, found_artist['name']) > 75):
                    return item['id']

    return None


def lambda_handler(event, context):
    
    print('event', event)

    if 'target_radio' not in event:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing target radio argument in the trigger event')
        }

    with open('config.json', 'r') as f:
        config = json.load(f)

    scrape_n_store(config, event['target_radio'])

    return {
        'statusCode': 200,
        'body': json.dumps('Function run successfully')
    }


# if __name__ == "__main__":

#     with open('config.json', 'r') as f:
#         config = json.load(f)

#     scrape_n_store(config, sys.argv[1])
