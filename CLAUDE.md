# Master-KI Hinweise — gh@pve (192.168.5.23)

## Wichtigste Regel: Usage-Limit schonen

Das Claude-Abo hat ein Stunden-Limit. Wenn du merkst, dass eine Aufgabe
**rein textbasiert** ist (Analyse, Zusammenfassung, einfache Codegenerierung,
Netzwerk-Lookup, Erklärung) — delegiere sie an das lokale Batch-Job-System
statt sie selbst zu bearbeiten. Das schont dein Kontingent und ist 54× günstiger.

---

## Batch-Job-System (Sub-Agent-Delegation)

**API-Endpunkt:** `http://192.168.5.23/api/batch/api.php`  
**Auth-Header:** `X-API-Key: 2a61f527ded09cc2832cb49f8829f299`  
**Web-UI:** `http://192.168.5.23/api/batch/`

### Job einreichen (curl)

```bash
curl -s -X POST http://192.168.5.23/api/batch/api.php \
  -H "X-API-Key: 2a61f527ded09cc2832cb49f8829f299" \
  -H "Content-Type: application/json" \
  -d '{
    "model":   "xiaomi",
    "prompt":  "Deine Aufgabe hier …",
    "targetdate": "'"$(date +%Y-%m-%d)"'"
  }'
# → {"id": 42, "status": "queued", "model": "xiaomi"}
```

### Job-Status prüfen

```bash
curl -s "http://192.168.5.23/api/batch/api.php?id=42&full=1&apikey=2a61f527ded09cc2832cb49f8829f299"
# → {"status":"done","result":"…","cost_usd":"0.000538"}
```

### Auf Ergebnis warten

```bash
while true; do
  STATUS=$(curl -s "http://192.168.5.23/api/batch/api.php?id=42&apikey=2a61f527ded09cc2832cb49f8829f299" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
  [ "$STATUS" = "done" ] || [ "$STATUS" = "failed" ] && break
  sleep 5
done
```

### Bis zu 9 Jobs parallel einreichen

```bash
for prompt in "Aufgabe 1" "Aufgabe 2" "Aufgabe 3"; do
  curl -s -X POST http://192.168.5.23/api/batch/api.php \
    -H "X-API-Key: 2a61f527ded09cc2832cb49f8829f299" \
    -H "Content-Type: application/json" \
    -d "{\"model\":\"xiaomi\",\"prompt\":\"$prompt\"}" &
done
wait
```

---

## Verfügbare Modelle

| model | Einsatz | Kosten |
|---|---|---|
| `xiaomi` | Standard-Sub-Agent: Analyse, Lookup, Zusammenfassung | ~$0.0005/Job |
| `mimo-pro` | Komplexe Reasoning-Aufgaben, lange Dokumente | ~$0.01/Job |
| `sonnet` | Hohe Qualität + Tool-Zugriff via Claude CLI | Abo-Limit |

**Faustregel:**
- Textaufgabe ohne Systemzugriff → `xiaomi`
- Reasoning/Analyse komplex → `mimo-pro`
- Tool-Zugriff / Datei-Operationen nötig → selbst erledigen (Claude CLI)

**NIEMALS `haiku` delegieren** — zu schwach für alle Aufgaben.

---

## Sub-Agent-Kontext (automatisch injiziert)

Jeder Job erhält automatisch als Prompt-Prefix:
- **`ki_localhost_cache`** — aktueller Systemzustand (RAM, Disk, OpenRouter-Guthaben)
- **`ki_infrastructure`** — alle Netzwerkgeräte mit IP, Ports, Zweck

Sub-Agents können daher direkt auf Geräte referenzieren (z. B. Kodi TV = `192.168.178.43`,
MQTT-Broker = `192.168.178.218:1883`) ohne dass du die IPs im Prompt angeben musst.

---

## Wann NICHT delegieren

- Aufgaben die Bash/Datei-Zugriff benötigen → selbst erledigen
- Interaktive Loops (der Sub-Agent antwortet einmalig, kein Dialog)
- Aufgaben < 5 Sekunden Denkzeit — Overhead lohnt sich nicht

---

## OpenRouter-Guthaben

Aktueller Stand in MariaDB:
```bash
mysql -u gh -pa12345 wagodb \
  -e "SELECT label, value FROM ki_localhost_cache WHERE category='openrouter' ORDER BY label"
```

---

## delegate — Direkt-Befehl ohne API-Key (empfohlen)

Statt curl: einfach `delegate` aufrufen — Key ist eingebaut.

```bash
delegate "Aufgabe …"                   # Job einreichen
delegate --wait "Aufgabe …"            # einreichen + auf Ergebnis warten
delegate --model mimo-pro "Aufgabe …"  # anderes Modell
delegate --list                         # letzte Jobs anzeigen
delegate --status 42                    # Job-Status + Ergebnis
```

Verfügbar auf jedem PC nach: `curl -O https://raw.githubusercontent.com/gerontec/claude-pro-scheduler/main/delegate.py && chmod +x delegate.py`

---

## Code vollständig lesen

Dateien unter 90 KB (ca. 3000 Zeilen) immer vollständig mit Read einlesen
bevor Fragen zur Logik, zum Verhalten oder zu Entscheidungen beantwortet werden.
Grep darf nur zur Orientierung genutzt werden (welche Datei, welche Zeile),
nie als alleinige Grundlage für inhaltliche Aussagen über Logik oder Verhalten.
