# radiofm-to-spotify-scraper

Collection of Python scripts used to scrape songs from Radio_FM web playlist to your Spotify playlist.

### Usage

- Scraping script reads songs from Radio_FM playlist, tries to find them on Spotify and then inserts them into SQL database. It is supposed to be run periodically (for example as a cron job) to incrementally fill the database.
- Downloading script can be run from commandline, and it just queries the database and inserts songs from specified range to your playlist.

File *config.json* holds all settings and auth ids.

Link to the radiofm playlist looks like this: `https://fm.rtvs.sk/playlist?page=1#playlist`. Any other RTVS radio station can be scraped the same way. 

### Required libraries:
- spotipy (spotify web-api communication)
- requests (download html)
- beautifulsoup4 (html parsing)
- fuzzywuzzy (fuzzy string matching)
- psycopg2 (communication with database)

*Side-note: Continuous scraping to database is necessary because RTVS changed their web playlists to only show last 24 hours of broadcast (it was unlimited before)*
