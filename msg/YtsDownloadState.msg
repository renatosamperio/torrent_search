Header header

## This is the torrent state
int8 STANDBY=1
int8 PAUSED=2
int8 CONFIGURING=3
int8 DOWNLOADING=4
int8 FINISHED_TORRENT=5

int8          fsm_state
int8          num_handles
YtsProgress[] torrents