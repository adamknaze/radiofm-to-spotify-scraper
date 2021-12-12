import sys
import json
import requests
from datetime import datetime
import itertools
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz
import psycopg2 as pg


def scrape_n_store(config, hours=13):
    '''
    Scrape tracks from RadioFM playlist, search last <HOURS> hours.
    Insert to db.
    '''

    # connect DB
    conn = pg.connect(
        host=config['db'],
        database='radiofm',
        user='radiofm',
        password=config['passw']
    )

    # scrape tracks
    tracks, timestamps = scrape_range(config['url'], hours)

    # find on spofity
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope='playlist-modify-public',
                                                   client_id=config['sp_client_id'],
                                                   client_secret=config['sp_client_secret'],
                                                   redirect_uri='http://localhost:8080/'))

    # insert into DB
    cur = conn.cursor()
    cur.execute('SELECT time FROM radiofm ORDER BY time DESC NULLS LAST LIMIT 1')
    maxtime_db = cur.fetchone()
    maxtime_db = maxtime_db[0].replace(tzinfo=None) if maxtime_db is not None else datetime.min

    for i in range(len(tracks)):
        if timestamps[i] < maxtime_db:
            continue

        try:
            track_id = find_on_spotify(sp, tracks[i][0], tracks[i][1])
        except Exception as e:    
            print(e) 
            track_id = None

        columns = ['time', 'artist', 'song']
        values = [str(timestamps[i]), tracks[i][0], tracks[i][1]]
        if track_id is not None:
            columns.append('spotify_id')
            values.append(track_id)
        values = ["E'" + x.replace("'", "\\'") + "'" for x in values]

        sql = "INSERT INTO radiofm (" + ', '.join(columns) + ") VALUES ("+ ', '.join(values) + ")"
        cur.execute(sql)

    conn.commit()
    cur.close()
    conn.close()


def find_on_spotify(sp, artist, song):

    track_id = None

    track_name_words = song.split()
    artist_name_words = artist.split()
    search_strings = []
    search_strings.append(song)
    search_strings.append(song + ' ' + artist)
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
                    track_id = item['id']
                    break

    return track_id


def scrape_range(main_url, hours):

    page = 1
    tracks, timestamps = scrape_page(main_url)
    
    while True:

        page += 1
        next_tracks, next_timestamps = scrape_page(main_url + '?page=' + str(page))
    
        if (timestamps[0] - next_timestamps[0]).total_seconds() / 60 / 60 > hours:
            break

        for i in range(len(next_tracks)):
            if not next_timestamps[i] in timestamps:
                tracks.append(next_tracks[i])
                timestamps.append(next_timestamps[i])

    return tracks, timestamps


def scrape_page(url):

    try:    
        html = requests.get(url)    
    except Exception as e:    
        print(e)    
        sys.exit(0)    

    tracks_list = []
    timestamps = []

    soup = BeautifulSoup(html.text, 'html.parser')
    rows = soup.find_all("table", {"class" : "table--playlist"}, limit=1)[0].find("tbody").find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        tracks_list.append((cells[2].get_text(), cells[3].get_text()))
        timestamp = '-'.join(list(reversed(cells[0].get_text().split('.')))) + ' ' + cells[1].get_text()
        timestamps.append(datetime.strptime(timestamp, '%Y-%m-%d %H:%M'))
    
    return tracks_list, timestamps


if __name__ == "__main__":

    with open('config.json', 'r') as f:
        config = json.load(f)

    scrape_n_store(config)
