# radiofm-to-spotify-importer
Small Python program that imports songs from radiofm playlist to your spotify playlist

Usage:
```
python radiofm_scraper.py <username> <radiofm playlist link> <playlist name (optional)>
```

Link to the radiofm playlist looks like this: `https://fm.rtvs.sk/playlist?page=32#playlist`.

Required libraries:
- spotipy (spotify web-api communication)
- requests (download html)
- beautifulsoup4 (html parsing)
- fuzzywuzzy (fuzzy string matching)

Program downloads the playlist page, then extracts all (usually 20) track-artist pairs. Each pair is searched for using
multiple search strings. If matched, it is then added to the specified playlist. Default playlist name is `radiofm_playlist`.
If no such playlist exists, new one is created.

Tested with Python 3.8
