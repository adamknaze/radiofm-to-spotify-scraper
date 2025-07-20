import sys
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
import time


def scrape_range(stat, stat_conf):

    if stat == 'radiofm':
        return rtvs_scrape_back(stat_conf['url'], stat_conf['hours'])
    elif stat == 'wave':
        return wave_scrape_date(stat_conf['url'], target_date=None) # default yesterday
    else:
        raise(Exception('Unrecognized station selected'))


def rtvs_scrape_back(url, hours):

    page = 1
    tracks, timestamps = rtvs_scrape_page(url)

    while True:

        page += 1
        next_tracks, next_timestamps = rtvs_scrape_page(f'{url}?page={page}')

        if (timestamps[0] - next_timestamps[0]).total_seconds() / 60 / 60 > hours:
            break

        for i in range(len(next_tracks)):
            if not next_timestamps[i] in timestamps:
                tracks.append(next_tracks[i])
                timestamps.append(next_timestamps[i])

    tracks_timestamps = []
    for i in range(len(tracks)):
        tracks_timestamps.append([tracks[i][0], tracks[i][1], timestamps[i]])

    return tracks_timestamps


def rtvs_scrape_page(url):

    try:    
        html = requests.get(url)
        # added reloads due to unreliable rtvs page behaviour
        time.sleep(1)
        html = requests.get(url)
        time.sleep(1.5)
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


def wave_scrape_date(url, target_date=None):

    if target_date is None: # yesterday
        the_date = date.today() - timedelta(days=1)
    else:
        the_date = target_date

    query_url = url.format(year=the_date.year, month=the_date.month, day=the_date.day)

    try:    
        json = requests.get(query_url)    
    except Exception as e:    
        print(e)    
        sys.exit(0)

    payload = json.json()

    tracks_timestamps = []

    for item in payload['data']:
        tracks_timestamps.append([item['interpret'], item['track'], datetime.fromisoformat(item['since'])])

    return tracks_timestamps
