import sys
import os
import argparse
import json
import spotipy
import spotipy.util as util
import psycopg2 as pg


def spotify_add_tracks(config, start, stop):

    # query DB
    conn = pg.connect(
        host=config['db'],
        database='radiofm',
        user='radiofm',
        password=config['passw']
    )

    cur = conn.cursor()
    cur.execute("SELECT spotify_id FROM radiofm WHERE spotify_id IS NOT NULL\
         AND time BETWEEN '" + start + "' AND '" + stop + "'")
    results = cur.fetchall()

    track_ids = [x[0] for x in results]

    # add on spotify
    token = util.prompt_for_user_token(config['user'], 'playlist-modify-public')
    if not token:
        print ("Can't get token for", config['user'])
        sys.exit(0)
    sp = spotipy.Spotify(auth=token)

    playlist_id = None
    playlists = sp.user_playlists(config['user'])
    for item in playlists['items']:
        if (item['name'] == config['playlist']):
            playlist_id = item['id']
    if (playlist_id is None):
        new_playlist = sp.user_playlist_create(config['user'], config['playlist'])
        playlist_id = new_playlist['id']

    sp.user_playlist_add_tracks(config['user'], playlist_id, track_ids)
    print('Succesfully added '+str(len(track_ids))+' songs to playlist '+config['playlist']+' of user '+config['user'])

    cur.close()
    conn.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Import songs scraped from radiofm to spotify playlist')
    subparsers = parser.add_subparsers(help='Select action')
    
    prs_conf = subparsers.add_parser('load', help='Load from config.json file')
    prs_conf.add_argument('-start', help='time from "1990-11-25 12:34"')
    prs_conf.add_argument('-stop', help='time from "1990-11-25 13:34"')
    prs_conf.set_defaults(action='load')

    prs_args = subparsers.add_parser('args', help='Supply args')
    prs_args.add_argument('-user', help='Spotify username')
    prs_args.add_argument('--playlist', default='radiofm_playlist', help='Spotify playlist')
    prs_args.add_argument('-start', help='time from "1990-11-25 12:34"')
    prs_args.add_argument('-stop', help='time from "1990-11-25 13:34"')
    prs_args.add_argument('-passw', help='DB password')
    prs_args.add_argument('-db', help='DB host')
    prs_args.add_argument('--url', default='https://fm.rtvs.sk/playlist', help='Radiofm playlist url')
    prs_args.set_defaults(action='args')

    args = parser.parse_args()

    if args.action == 'load' and os.path.exists('config.json'):
        with open('config.json', 'r') as f:
            config = json.load(f)

    else:
        config = {
            'user': args.user,
            'playlist': args.playlist,
            'pass': args.passw,
            'db': args.db,
            'url': args.url
        }
    
    spotify_add_tracks(config, args.start, args.stop)
