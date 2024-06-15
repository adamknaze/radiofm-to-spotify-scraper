import time
from datetime import datetime, timedelta
from collections import OrderedDict
import json
from datetime import datetime
import itertools
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
from spotipy.cache_handler import MemoryCacheHandler
from fuzzywuzzy import fuzz
import psycopg2 as pg

from radio_scrapers import scrape_range
from PostgresCacheHandler import PostgresCacheHandler

CHUNK_SIZE = 95


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


def spotify_daily_add_tracks(config, playlist_id, station='radiofm', days=[-1], start='07:00', stop='18:00', nofilter=False):

    now = datetime.now()
    dates = []
    for day in days:
        if day < 0:
            dt_day = now - timedelta(days=-day)
            dates.append({'d': dt_day.day, 'm': dt_day.month, 'y': dt_day.year})
        else:
            dates.append({'d': day, 'm': now.month, 'y': now.year})

    # query DB
    conn = pg.connect(
        host=config['db_host'],
        database=config['db'],
        user=config['db_user'],
        password=config['passw']
    )

    time_ranges = [f"time BETWEEN '{x['y']}-{x['m']}-{x['d']} {start}' AND '{x['y']}-{x['m']}-{x['d']} {stop}'" for x in dates]

    cur = conn.cursor()
    cur.execute(f"SELECT spotify_id FROM {config['stations'][station]['db_table']} WHERE spotify_id IS NOT NULL AND ({' OR '.join(time_ranges)}) ORDER BY time;")
    results = cur.fetchall()

    track_ids = [x[0] for x in results if x[0]]

    # non-consecutive duplicity filter turned ON by default (while I improve the consecutive duplicity removing)
    if not nofilter:
        track_ids = list(OrderedDict.fromkeys(track_ids))

    # remove consecutive duplicates # TODO extend to duplications within a small range
    # track_ids = [x[0] for x in itertools.groupby(track_ids)]

    # Add on spotify
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope='playlist-modify-public',
                                                   client_id=config['sp_client_id'],
                                                   client_secret=config['sp_client_secret'],
                                                   redirect_uri='http://localhost:8080/',
                                                   cache_handler=PostgresCacheHandler(conn))      # used in Lambda
    )


    playlists = sp.user_playlists(config['user'])
    playlist_name = 'not found'
    for item in playlists['items']:
        if item['id'] == playlist_id:
            playlist_name = item['name']
            break


    remove_all_tracks_from_playlist(sp, config['user'], playlist_id)


    if not len(track_ids) == 0:
        for i in range(0, len(track_ids), CHUNK_SIZE):
            sp.user_playlist_add_tracks(config['user'], playlist_id, track_ids[i:i + CHUNK_SIZE])
            time.sleep(1)
        print('Succesfully added '+str(len(track_ids))+' songs to playlist '+playlist_name+' of user '+config['user'])
    else:
        print('No Spotify tracks found in selected range.')

    cur.close()
    conn.close()


def remove_all_tracks_from_playlist(sp, user, playlist_id):

    results = sp.user_playlist_tracks(user, playlist_id)
    tracks = results['items']

    while results['next']:
        results = sp.next(results)
        tracks.extend(results['items'])

    track_uris = [track['track']['uri'] for track in tracks]

    for i in range(0, len(track_uris), CHUNK_SIZE):
        sp.user_playlist_remove_all_occurrences_of_tracks(user, playlist_id, track_uris[i:i + CHUNK_SIZE])


def lambda_handler(event, context):

    if 'target_radio' not in event:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing target radio argument in the trigger event')
        }

    with open('config.json', 'r') as f:
        config = json.load(f)
    
    if 'update_daily' not in event:

        scrape_n_store(config, event['target_radio'])

    else:
        args = config['stations'][event['target_radio']]['auto_playlists'][event['update_daily']]

        days = [-1] if 'days' not in args else args['days']
        start = '07:00' if 'start' not in args else args['start']
        stop = '18:00' if 'stop' not in args else args['stop']

        spotify_daily_add_tracks(config, args['playlist_id'], event['target_radio'], days, start, stop)

    return {
        'statusCode': 200,
        'body': json.dumps('Function run successfully')
    }


# if __name__ == "__main__":

#     with open('config.json', 'r') as f:
#         config = json.load(f)

#     lambda_handler({'target_radio': 'radiofm', 'update_daily': 'radiofm_vcera_all'}, None)
