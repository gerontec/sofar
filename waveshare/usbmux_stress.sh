#!/bin/bash
# ════════════════════════════════════════════════════════════════════════════
#  usbmux_stress.sh — Belastungs- und Korrektheitstest für den USB-Mux
#
#  Prüft nicht nur "läuft durch", sondern das, was beim Multiplexen wirklich
#  schiefgehen kann: dass eine Log-Zeile in einer Kommandoantwort landet oder
#  umgekehrt, dass parallele Anfragen sich die Antworten klauen, dass ein
#  überlanger Befehl den Zeilenparser aus dem Tritt bringt.
#
#  Voraussetzung: usbmux daemon läuft.  Aufruf:  ./usbmux_stress.sh
# ════════════════════════════════════════════════════════════════════════════
set -u

MUX=${MUX:-./usbmux}
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
fail=0

ok()   { printf '  \033[32mok\033[0m    %s\n' "$1"; }
bad()  { printf '  \033[31mFEHLER\033[0m %s\n' "$1"; fail=$((fail+1)); }
head_() { printf '\n\033[1m%s\033[0m\n' "$1"; }

$MUX state >/dev/null 2>&1 || { echo "kein Daemon — erst './usbmux daemon &'"; exit 1; }

# ── 1. Durchsatz Kanal 1: viele Statusabfragen hintereinander ───────────────
head_ "1. Kanal 1 Durchsatz (100x status, längste Antwort im System)"
n=100; good=0; t0=$(date +%s.%N)
for i in $(seq $n); do
	r=$($MUX cmd status 2>/dev/null)
	grep -q '"fw":"' <<<"$r" && grep -q '^+OK' <<<"$r" && good=$((good+1))
done
t1=$(date +%s.%N)
dur=$(echo "$t1 - $t0" | bc)
rate=$(echo "scale=1; $good / $dur" | bc)
echo "  $good/$n vollständig in ${dur}s (${rate}/s)"
[ "$good" = "$n" ] && ok "keine Antwort verloren oder verstümmelt" \
                   || bad "$((n-good)) Antworten unvollständig"

# ── 2. Parallelität: gleichzeitige Anfragen dürfen sich nicht vermischen ────
head_ "2. 20 gleichzeitige Kommandos (FIFO muss serialisieren)"
for i in $(seq 20); do
	( $MUX cmd ping > "$TMP/par_$i.txt" 2>&1 ) &
done
wait
pgood=0
for i in $(seq 20); do
	# jede Antwort muss genau ein pong und genau ein +OK enthalten
	p=$(grep -c '^pong ' "$TMP/par_$i.txt")
	o=$(grep -c '^+OK$'  "$TMP/par_$i.txt")
	[ "$p" = 1 ] && [ "$o" = 1 ] && pgood=$((pgood+1))
done
echo "  $pgood/20 Antworten sauber zugeordnet"
[ "$pgood" = 20 ] && ok "keine Vermischung zwischen parallelen Anfragen" \
                  || bad "$((20-pgood)) Antworten vertauscht/unvollständig"

# ── 3. Kanaltrennung unter Last ────────────────────────────────────────────
head_ "3. Kanaltrennung: Monitor mitlaufen lassen, währenddessen Kommandos"
timeout 20 $MUX mon > "$TMP/mon.txt" 2>&1 &
monpid=$!
sleep 1
for i in $(seq 15); do
	$MUX cmd relay $((i % 6 + 1)) $((i % 2)) > "$TMP/cmd_$i.txt" 2>&1
	$MUX cmd wifi >> "$TMP/cmd_$i.txt" 2>&1
done
$MUX cmd scan > "$TMP/cmd_scan.txt" 2>&1     # erzeugt INFO-Zeilen auf Kanal 2
sleep 3
kill $monpid 2>/dev/null; wait $monpid 2>/dev/null

leak_k1=$(grep -c '#K1#' "$TMP/mon.txt" || true)
leak_log=$(cat "$TMP"/cmd_*.txt | grep -c '\[I\]\[\|\[W\]\[\|\[D\]\[' || true)
echo "  Kanal-1-Zeilen im Log-Monitor:  $leak_k1  (soll 0)"
echo "  Log-Zeilen in Kommandoantworten: $leak_log  (soll 0)"
[ "$leak_k1" = 0 ]  && ok "kein Steuerverkehr im Log" || bad "Steuerzeilen im Log gelandet"
[ "$leak_log" = 0 ] && ok "kein Log in den Antworten" || bad "Log-Zeilen in Antworten gelandet"
grep -q 'open_wifi' "$TMP/mon.txt" && ok "Log kam beim Monitor an" \
                                   || bad "Monitor hat nichts gesehen"
$MUX cmd relay all 0 >/dev/null 2>&1

# ── 4. Viele Monitore gleichzeitig ─────────────────────────────────────────
head_ "4. 12 Monitore gleichzeitig"
for i in $(seq 12); do
	( timeout 8 $MUX mon > "$TMP/m_$i.txt" 2>&1 ) &
done
sleep 2
$MUX cmd scan >/dev/null 2>&1
sleep 4
wait
seen=0
for i in $(seq 12); do
	[ -s "$TMP/m_$i.txt" ] && seen=$((seen+1))
done
echo "  $seen/12 Monitore haben Daten bekommen"
[ "$seen" = 12 ] && ok "alle Monitore versorgt" || bad "$((12-seen)) Monitore leer"

# ── 5. Grenzfälle des Zeilenparsers ────────────────────────────────────────
head_ "5. Grenzfälle"
long=$(printf 'x%.0s' $(seq 300))
r=$($MUX cmd "$long" 2>&1)
grep -q 'zu lang' <<<"$r" && ok "300-Zeichen-Zeile sauber abgewiesen (UM_MAX_LINE 160)" \
                          || bad "Überlange Zeile: unerwartete Antwort: $(head -1 <<<"$r")"
r=$($MUX cmd gibtsnicht 2>&1)
grep -q 'unbekanntes Kommando' <<<"$r" && ok "unbekanntes Kommando -> -ERR" \
                                       || bad "unbekanntes Kommando: $r"
r=$($MUX cmd relay 9 1 2>&1)
grep -q -- '-ERR' <<<"$r" && ok "ungültiger Relaiskanal -> -ERR" \
                          || bad "relay 9 wurde nicht abgewiesen"
r=$($MUX cmd loglevel VERBOSE 2>&1)
grep -q 'nicht einkompiliert' <<<"$r" && ok "loglevel über Compile-Level -> -ERR" \
                                      || bad "loglevel VERBOSE: $r"

# ── 6. Port abgeben und zurücknehmen ───────────────────────────────────────
head_ "6. pause/resume (Portfreigabe fürs Flashen)"
$MUX pause >/dev/null 2>&1
r=$($MUX cmd ping 2>&1)
grep -q 'pausiert' <<<"$r" && ok "im Pausenzustand sauberer Fehler statt Hänger" \
                           || bad "pause: $r"
$MUX resume >/dev/null 2>&1
sleep 2
r=$($MUX cmd ping 2>&1)
grep -q '^pong' <<<"$r" && ok "nach resume wieder erreichbar" || bad "resume: $r"

# ── Ergebnis ───────────────────────────────────────────────────────────────
head_ "Ergebnis"
if [ "$fail" = 0 ]; then
	printf '  \033[32malle Prüfungen bestanden\033[0m\n'
else
	printf '  \033[31m%d Prüfung(en) fehlgeschlagen\033[0m\n' "$fail"
fi
exit $fail
