import os
import sys
import re
from datetime import datetime, timedelta
import time
import argparse
import json
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import psycopg2 as pg

CHUNK_SIZE = 95

time_re = re.compile('[0-2]{1}[0-9]{1}:[0-9]{2}')


def spotify_add_tracks(config, station, days, start, stop, nofilter=False):

    # query DB
    conn = pg.connect(
        host=config['db_host'],
        database=config['db'],
        user=config['db_user'],
        password=config['passw']
    )

    time_ranges = [f"time BETWEEN '{x['y']}-{x['m']}-{x['d']} {start}' AND '{x['y']}-{x['m']}-{x['d']} {stop}'" for x in days]
    print(f'Downloading songs played on {station} in following time ranges:')
    for t_range in time_ranges:
        print(t_range)

    cur = conn.cursor()
    cur.execute(f"SELECT spotify_id FROM {config[station]['db_table']} WHERE spotify_id IS NOT NULL AND ({' OR '.join(time_ranges)}) ORDER BY time;")
    results = cur.fetchall()

    track_ids = [x[0] for x in results if x[0]]

    if not nofilter:
        track_ids = list(set(track_ids))

    # Add on spotify
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope='playlist-modify-public',
                                                   client_id=config['sp_client_id'],
                                                   client_secret=config['sp_client_secret'],
                                                   redirect_uri='http://localhost:8080/'))

    playlist_id = None
    playlists = sp.user_playlists(config['user'])
    for item in playlists['items']:
        if (item['name'] == config['playlist']):
            playlist_id = item['id']
    if (playlist_id is None):
        new_playlist = sp.user_playlist_create(config['user'], config['playlist'])
        playlist_id = new_playlist['id']

    if not len(track_ids) == 0:
        for i in range(0, len(track_ids), CHUNK_SIZE):
            sp.user_playlist_add_tracks(config['user'], playlist_id, track_ids[i:i + CHUNK_SIZE])
            time.sleep(1)
        print('Succesfully added '+str(len(track_ids))+' songs to playlist '+config['playlist']+' of user '+config['user'])
    else:
        print('No Spotify tracks found in selected range.')

    cur.close()
    conn.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Import songs scraped from radiofm to spotify playlist')
    parser.add_argument('-c', '--config', default='config.json', help='config file path')
    parser.add_argument('-y', '--year', help='year, current by default')
    parser.add_argument('-m', '--month', help='month, current by default')
    parser.add_argument('-d', '--day', nargs='+', help='day (or multiple)')
    parser.add_argument('-l', '--last', help='select *n* last days starting with yesterday', default='1')
    parser.add_argument('-s', '--start', help='start time hh:mm', default='07:00')
    parser.add_argument('-e', '--end', help='end time hh:mm', default='16:00')
    parser.add_argument('-nf', '--nofilter', action='store_true', help='If set, duplicitous songs will NOT be removed')
    parser.add_argument('-s', '--station', default='radiofm', help='radio station to be downloaded')

    args = parser.parse_args()

    if os.path.exists(args.config):
        with open(args.config, 'r') as f:
            config = json.load(f)
    else:
        print(f'The config file "{args.config}" not found')
        sys.exit()

    now = datetime.now()

    year = now.year if args.year is None else args.year
    month = now.month if args.month is None else args.month

    if args.day is not None and len(args.day) > 0:
        days = [{'d': x, 'm': month, 'y': year} for x in args.day]

    else:
        days = []
        for i in range(1, int(args.last) + 1):
            dt_day = now - timedelta(days=i)
            days.append({'d': dt_day.day, 'm': dt_day.month, 'y': dt_day.year})

    start, end = args.start, args.end

    if time_re.match(start) is None or time_re.match(end) is None:
        print('Wrong time format used')
        sys.exit()

    spotify_add_tracks(config, args.station, days, start, end, nofilter=args.nofilter)
