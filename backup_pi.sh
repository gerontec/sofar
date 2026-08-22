#!/bin/bash
# backup_pi.sh - taeglicher rsync-Pull von /home/pi/{python,perl} eines Pi
# nach /home/gh/backup/<label>/.
#
#   Aufruf: backup_pi.sh <label> <host> [fallback-host ...]
#   z.B.    backup_pi.sh 218 192.168.178.218
#           backup_pi.sh 119 192.168.178.119 192.168.178.123
#
# Pull statt Push: so braucht kein Pi einen Schluessel auf diese Maschine, und
# wer einen Pi uebernimmt, kommt an das Backup nicht heran.
#
# Mehrere Hosts = derselbe Rechner ueber verschiedene Wege (raspberrypi haengt
# mit wlan0 auf .119 und eth0 auf .123, gleicher Host-Key). Genommen wird der
# erste, der eine SSH-Verbindung annimmt -- geprueft wird die Anmeldung, nicht
# nur ein Ping: ein Interface kann antworten, waehrend sshd nicht darauf hoert.
#
# Gespiegelt wird mit --delete, aber nichts geht verloren: was geloescht oder
# ueberschrieben wird, landet vorher in _attic/<Datum>/ und faellt erst nach
# KEEP_DAYS heraus. Ein versehentliches rm auf dem Pi ist damit noch heilbar.
set -u

# Compiler-Ausgabe wird nicht gesichert: sie ist aus den Quellen wieder
# herstellbar und machte auf .218 allein 1,9 GB der 2,3 GB aus (ESP-IDF/matter).
# managed_components sind nachgeladene Fremdkomponenten (idf.py reconfigure
# holt sie zurueck). Ohne --delete-excluded, damit rsync nichts ins Attic
# schaufelt -- Altbestand wird einmalig von Hand entfernt.
EXCLUDES=(--exclude=build/ --exclude=.pio/ --exclude=managed_components/
          --exclude=__pycache__/ --exclude='*.pyc')

LABEL=${1:?Aufruf: backup_pi.sh <label> <host> [fallback-host ...]}
shift
HOSTS="$@"
[ -n "$HOSTS" ] || { echo "kein Host angegeben"; exit 2; }

SRC_USER=pi
DIRS="python perl"
DEST=/home/gh/backup/$LABEL
DAY=$(date +%F)
ATTIC=$DEST/_attic/$DAY
LOG=$DEST/_log/rsync-$DAY.log
KEEP_DAYS=30
SSH="ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=15"

mkdir -p "$DEST/_log" "$ATTIC"
exec 9>"$DEST/.lock"
# Ein Lauf haengt gelegentlich am Netz; der naechste soll dann nicht danebenlaufen.
flock -n 9 || { echo "$(date '+%F %T') $LABEL laeuft bereits - uebersprungen" >>"$LOG"; exit 0; }

HOST=""
for h in $HOSTS; do
    if $SSH -o ConnectTimeout=5 "$SRC_USER@$h" true >/dev/null 2>&1; then
        HOST=$h; break
    fi
    echo "$(date '+%F %T') $h nicht erreichbar - naechster Weg" >>"$LOG"
done
if [ -z "$HOST" ]; then
    echo "$(date '+%F %T') $LABEL: kein Weg erreichbar ($HOSTS)" | tee -a "$LOG"
    exit 3
fi

rc_all=0
for d in $DIRS; do
    echo "=== $(date '+%F %T') $SRC_USER@$HOST:/home/pi/$d" >>"$LOG"
    rsync -a --delete --numeric-ids --human-readable --stats \
          "${EXCLUDES[@]}" \
          --backup --backup-dir="$ATTIC/$d" \
          -e "$SSH" \
          "$SRC_USER@$HOST:/home/pi/$d/" "$DEST/$d/" >>"$LOG" 2>&1
    rc=$?
    # 24 = "Datei war beim Uebertragen schon wieder weg" (Logs, __pycache__).
    # Das ist bei einem laufenden System normal und kein Fehlschlag.
    [ $rc -eq 24 ] && rc=0
    [ $rc -ne 0 ] && rc_all=$rc
    echo "--- rc=$rc" >>"$LOG"
done

# Leere Attic-Tage gar nicht erst stehen lassen, alte wegraeumen.
rmdir "$ATTIC"/* "$ATTIC" 2>/dev/null
find "$DEST/_attic" -mindepth 1 -maxdepth 1 -type d -mtime +$KEEP_DAYS \
     -exec rm -rf {} + 2>/dev/null
find "$DEST/_log" -name 'rsync-*.log' -mtime +$KEEP_DAYS -delete 2>/dev/null

size=$(du -sh "$DEST/python" "$DEST/perl" 2>/dev/null | awk '{printf "%s ", $1}')
echo "$(date '+%F %T') $LABEL fertig via $HOST rc=$rc_all ($size)" | tee -a "$LOG"
exit $rc_all
