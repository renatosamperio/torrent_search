Header header
string title
int32  seeds
string hash
string imdb
int32  torrent_id
string date_uploaded

uint8 SEARCH_BY_SEEDS=1
uint8 SEARCH_BY_TITLE=2
uint8 SEARCH_BY_SEEDS_NOT_DOWNLOADED=3
uint8 SEARCH_BY_ID=4
uint8 SEARCH_BY_HASH=5
uint8 SEARCH_BY_IMDB_CODE=6
uint8 SEARCH_FILTERED=100
uint8 SEARCH_FILTERED_NOT_DOWNLOADED_BY_SEEDS=101
uint8 SEARCH_FILTERED_NOT_DOWNLOADED_BY_DATE=102
uint8 search_type