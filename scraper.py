import sys
import json
import requests
from datetime import datetime
import itertools
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.cache_handler import MemoryCacheHandler
from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz
import psycopg2 as pg


def scrape_n_store(config, hours=5):
    '''
    Scrape tracks from RadioFM playlist, search last <HOURS> hours.
    Insert to db.
    '''

    # connect DB
    conn = pg.connect(
        host=config['db_host'],
        database=config['db'],
        user=config['db_user'],
        password=config['passw']
    )

    # scrape tracks
    tracks, timestamps = scrape_range(config['url'], hours)

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
    cur.execute('SELECT time FROM radiofm ORDER BY time DESC NULLS LAST LIMIT 1')
    maxtime_db = cur.fetchone()
    maxtime_db = maxtime_db[0].replace(tzinfo=None) if maxtime_db is not None else datetime.min

    insert_data = []

    for i in range(len(tracks)):
        if timestamps[i] < maxtime_db:
            continue

        try:
            track_id = find_on_spotify(sp, tracks[i][0], tracks[i][1])
        except Exception as e:
            print(e) 
            track_id = None
        
        insert_data.append([str(timestamps[i]), tracks[i][0].replace("'", "''"), tracks[i][1].replace("'", "''"), track_id])

    tick = "'"
    insert_strs = [f"('{x[0]}', '{x[1]}', '{x[2]}', {tick + x[3] + tick if x[3] is not None else 'NULL'})" for x in insert_data]
    val_str = ', '.join(insert_strs)

    sql = f"""
        INSERT INTO radiofm (time, artist, song, spotify_id) VALUES {val_str}
        ON CONFLICT ON CONSTRAINT radiofm_time_artist_song_key DO NOTHING;
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
