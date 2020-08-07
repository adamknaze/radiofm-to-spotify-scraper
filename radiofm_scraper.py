import sys
import requests
import itertools
import spotipy
import spotipy.util as util
from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz


def spotify_add_tracks(username, playlist_name, tracks_list):
    scope = 'playlist-modify-public'

    # token = util.prompt_for_user_token(username, scope, client_id='client_id',
    #                                                     client_secret='client_secret',
    #                                                     redirect_uri='http://localhost/')

    token = util.prompt_for_user_token(username, scope)

    if token:
        sp = spotipy.Spotify(auth=token)

        playlist_id = ''
        playlists = sp.user_playlists(username)
        for item in playlists['items']:
            if (item['name'] == playlist_name):
                playlist_id = item['id']
        if (playlist_id == ''):
            new_playlist = sp.user_playlist_create(username, playlist_name)
            playlist_id = new_playlist['id']

        print('playlist prepared, starting matching')

        tracks_ids = []
        matched = False
        for track in tracks_list:

            track_name_words = track[0].split()
            artist_name_words = track[1].split()
            search_strings = []
            search_strings.append(track[0])
            search_strings.append(track[0]+' '+track[1])
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
                if (fuzz.token_set_ratio(track[0], item['name']) > 75):
                    for artist in item['artists']:
                        if (fuzz.token_set_ratio(track[1], artist['name']) > 75):
                            tracks_ids.append(item['id'])
                            matched = True
                            break
                if (matched):
                    print('successfully matched song '+track[0]+' by '+track[1])
                    break
            if (not matched):
                print('---couldn\'t match song '+track[0]+' by '+track[1])
            matched = False
        
        sp.user_playlist_add_tracks(username, playlist_id, tracks_ids)
        print('Succesfully added '+str(len(tracks_ids))+' songs to playlist '+playlist_name+' of user '+username)

    else:
        print ("Can't get token for", username)

def radiofm_scrape_tracks(url):
    try:    
        html = requests.get(url)    
    except Exception as e:    
        print(e)    
        sys.exit(0)    

    print('downloaded playlist at '+url)
    tracks_list = []

    soup = BeautifulSoup(html.text, 'html.parser')
    rows = soup.find_all("table", {"class" : "table--playlist"}, limit=1)[0].find("tbody").find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        tracks_list.append((cells[3].get_text(), cells[2].get_text()))

    print('parsed '+str(len(tracks_list))+' rows')
    return tracks_list


if __name__ == "__main__":

    if len(sys.argv) > 3:
        username = sys.argv[1]
        radiofm_url = sys.argv[2]
        playlist_name = sys.argv[3]
    elif len(sys.argv) > 2:
        username = sys.argv[1]
        radiofm_url = sys.argv[2]
        playlist_name = "radiofm_playlist"
    else:
        print ("Usage: %s username radiofm_url (optional)playlist_name" % (sys.argv[0],))
        sys.exit()

    tracks_list = radiofm_scrape_tracks(radiofm_url)
    spotify_add_tracks(username, playlist_name, tracks_list)
