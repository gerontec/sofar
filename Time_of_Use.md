# Sofar HYD — Time of Use (TOU) / Zeittarif

Stand: 2026-08-12, ausgelesen über Modbus RTU (`/dev/ttyUSB32`, 9600 8N1, Unit 1),
Registerblock `0x1110`–`0x112F`. **Reine Lesezugriffe, nichts geschrieben.**

Quelle der Registerdefinition: `/home/gh/python/SofarRegisters.xlsx` (Vollversion mit
Wertebereichen und Beschreibungstexten). Die auf dem Pi liegende
`sofarregister.csv` ist ein reduzierter Export ohne Bereichs-/Beschreibungsspalten;
die Einheiten stehen dort chinesisch (小时 = Stunde, 分钟 = Minute, 月 = Monat, 日 = Tag).

---

## 1. Ist-Zustand des Wechselrichters

### Betriebsmodus — das Entscheidende zuerst

| Register | Feld | Wert | Bedeutung |
|---|---|---|---|
| `0x1110` | Energy_Storage_Mode_Control | **0** | **Eigenverbrauch** (self-generation and self-consumption) |

Werteliste: `0` = Eigenverbrauch · `1` = Zeittarif (TOU) · `2` = Zeitgesteuertes
Laden/Entladen · `3` = Passiv · `4` = Peak-Shaving.

> **Die TOU-Regel unten ist gespeichert und per `TOU_On_Off_Control` scharf, aber
> sie wirkt nicht**, weil der Wechselrichter im Eigenverbrauchsmodus läuft. Erst
> `0x1110 = 1` würde den Zeittarif aktiv schalten.

### TOU-Regel 0 (`0x1120`–`0x112F`)

| Register | Feld | Roh | Dekodiert |
|---|---|---|---|
| `0x1120` | TOU_ID | 0 | Regel-Nr. 0 (von 0–7; kleinere Nr. = höhere Priorität) |
| `0x1121` | TOU_On_Off_Control | 1 | **aktiviert** |
| `0x1122` | TOU_Charge_Start | `0x0800` | **08:00** |
| `0x1123` | TOU_Charge_End | `0x1228` | **18:40** |
| `0x1124` | TOU_Charge_Target_SOC | 10 | **10 %** — unterhalb des dokumentierten Minimums von 30 % |
| `0x1125/26` | TOU_Charge_Power (U32) | 2500 | **2500 W** |
| `0x1127` | TOU_Executed_Date_Start | `0x0101` | **01.01.** |
| `0x1128` | TOU_Executed_Date_End | `0x0C1F` | **31.12.** |
| `0x1129` | TOU_Executed_Day_of_Week | `0x7F` | **Mo–So** (alle 7 Tage) |
| `0x112A`–`0x112E` | TOU_Rsvd1–5 | 0 | reserviert |
| `0x112F` | TOU_Control | 0 | Status des letzten Schreibvorgangs: 0 = erfolgreich |

Ziel-SOC 10 % bedeutet: Zwangsladung endet, sobald der Akku 10 % erreicht. Da der
SOC praktisch immer darüber liegt, würde die Regel selbst bei aktivem Zeittarif-Modus
nie laden.

### Timed charging/discharging (`0x1110`–`0x111F`) — zum Vergleich

| Register | Feld | Roh | Dekodiert |
|---|---|---|---|
| `0x1111` | Timing_ID | 0 | Regel-Nr. 0 (von 0–3) |
| `0x1112` | Timing_On_Off_Control | 0 | **Laden aus, Entladen aus** (Bit0 = Laden, Bit1 = Entladen) |
| `0x1113` | Timing_Charge_Start | `0x1600` | 22:00 |
| `0x1114` | Timing_Charge_End | `0x051E` | 05:30 |
| `0x1115` | Timing_Discharge_Start | `0x0600` | 06:00 |
| `0x1116` | Timing_Discharge_End | `0x1500` | 21:00 |
| `0x1117/18` | Timing_Power_Charge (U32) | 1000 | 1000 W |
| `0x1119/1A` | Timing_Power_Discharge (U32) | 1000 | 1000 W |
| `0x111F` | Timing_Control | 0 | letzter Schreibvorgang erfolgreich |

Ebenfalls inaktiv: `0x1110` steht nicht auf 2, und beide Enable-Bits sind 0.

---

## 2. Kodierung der Register (aus der Doku)

- **Zeitfelder** (`0x1113`–`0x1116`, `0x1122`, `0x1123`): High Byte = Stunde (0–23),
  Low Byte = Minute (0–59). `0x0800` = 08:00, `0x1228` = 18:40.
- **Datumsfelder** (`0x1127`, `0x1128`): High Byte = Monat (1–12), Low Byte = Tag (1–31).
- **Wochentagsmaske** (`0x1129`): Bitfeld, **Bit0 = Montag** … Bit6 = Sonntag.
  `0x7F` = alle Tage.
- **Leistungsfelder**: U32 über zwei Register, Einheit W.
- `TOU_ID`/`Timing_ID`: Beim **Schreiben** wird die Regel dieser Nummer in die
  Schattenregister geladen, `0x1122` ff. zeigen danach diese Regel. Ohne Schreibzugriff
  sieht man die zuletzt geladene Regel — hier Regel 0.
- `TOU_Control`/`Timing_Control`: Schreibwert 1 überträgt die Schattenregister in die
  Systemkonfiguration; beim Lesen Statuscode des letzten Schreibvorgangs
  (0 = Erfolg, 1 = Operation fehlgeschlagen).
- `TOU_On_Off_Control` (`0x1121`) ist laut Doku **RW** mit Bereich 0/1. Der Kommentar
  in `tou_disable.py` („read-only from Modbus") stimmt nicht.

---

## 3. Bugfix 2026-08-12: Zeit-/Datumsregister waren um Faktor 11 verfälscht

`read.py` (und Geschwister) leiteten den Skalierungsfaktor aus Spalte 5 der
`sofarregister.csv` ab. Für Register mit zwei Teilwerten steht dort ein
**zweizeiliger** Wert — `"1\n1"` für Stunde/Minute bzw. Monat/Tag. Der alte Code

```python
accuracy_str = row[4].replace(',', '.')
accuracy = float(''.join(filter(lambda x: x.isdigit() or x == '.', accuracy_str)))
```

filterte das Newline heraus und machte aus `"1\n1"` die Zahl **11**. Alle Zeit- und
Datumsregister wurden dadurch mit 11 multipliziert:

| Feld | echt | geloggt (×11) |
|---|---|---|
| TOU_Charge_Start | 2048 (`0x0800`) | 22528 |
| TOU_Charge_End | 4648 (`0x1228`) | 51128 |
| TOU_Executed_Date_Start | 257 (`0x0101`) | 2827 |
| TOU_Executed_Date_End | 3103 (`0x0C1F`) | 34133 |

Nicht betroffen waren Felder mit einzeiliger Accuracy (`TOU_Charge_Target_SOC`,
Leistungen) und solche ohne Accuracy (`TOU_On_Off_Control`, `TOU_ID`).

**Fix** (idempotent, nur die erste Zeile auswerten):

```python
accuracy_str = row[4].replace(',', '.').splitlines()[0] if row[4].strip() else ''
```

Gepatcht am 2026-08-12, Backups als `*.bak_acc20260812`:

- Pi `/home/pi/python/`: `read.py` (produktiv, via `sofar.sh`-Cron), `read64.py`,
  `read9000.py`, `read_sofar2.py`
- Arbeitsplatz `/home/gh/python/`: `read.py`, `read64.py`, `read9000.py`,
  `read_sofar2.py`, `readtcp.py`, `readtcp2.py`, `sofar_pivot.py`
- Arbeitsplatz `/home/gh/sofar/` (ältere Zweitkopien): `read.py`, `read_sofar2.py`,
  `sofar_pivot.py`

`pivot2db.py` war nicht betroffen — es liest `/tmp/pivoted_registers.csv` und reicht
die Werte ohne eigene Skalierung in die Tabelle durch (Spaltentyp `DECIMAL(10,4)`,
für Werte bis 4648 unkritisch). Mit dem korrigierten `read.py` landet also automatisch
der richtige Wert in der DB. Verifiziert am ersten Poll nach dem Patch:

```
2026-08-12 17:36:09   2048.0   4648.0   257.0   3103.0    <- korrekt
2026-08-12 17:35:09  22528.0  51128.0  2827.0  34133.0    <- alt, x11
```

---

## 4. Schema-Umstellung 2026-08-12: Uhrzeiten als TIME

`inverter_data` speichert die Zeitregister jetzt als echten `TIME`-Typ statt als
Rohzahl. `TOU_Charge_Start` liest sich damit als `08:00:00` statt `2048`.

| Spalte | vorher | nachher |
|---|---|---|
| `TOU_Charge_Start` | float | **time** |
| `TOU_Charge_End` | float | **time** |
| `Timing_Charge_Start` / `_End` | float | **time** (durchgehend NULL, nur Typwechsel) |
| `Timing_Discharge_Start` / `_End` | float | **time** (dito) |
| `TOU_Executed_Date_Start` / `_End` | float | **int** als `YYYYMMDD` (`20260101` / `20261231`) |

Die Altdaten wurden dabei mitkorrigiert: Zeilen vor dem `read.py`-Fix zuerst durch 11
geteilt, dann `SEC_TO_TIME(Stunde*3600 + Minute*60)`; Werte außerhalb 0–23/0–59
wurden NULL.

### Datumsspalten als YYYYMMDD

Die Regel selbst trägt kein Jahr (nur Monat/Tag), gespeichert wird deshalb
`YYYYMMDD` mit dem Jahr der jeweiligen Zeile: `257` → **20260101**,
`3103` → **20261231**. Umgestellt: 203 817 Zeilen (`_Start`) bzw. 189 563 (`_End`),
Backup der Rohwerte in **`inverter_data_toudate_bak20260812`**.

Der Typwechsel `float` → `int` ist dabei zwingend, nicht kosmetisch: MySQLs `float`
ist einfach genau und stellt ganze Zahlen nur bis 16 777 216 exakt dar. Achtstellige
Datumszahlen liegen darüber und würden verfälscht:

```
     257 -> float32        257.0  exakt
20260101 -> float32   20260100.0  VERFAELSCHT
20261231 -> float32   20261232.0  VERFAELSCHT
```

In `pivot2db.py` erledigt das `DATE_COLUMNS` + `register_to_yyyymmdd()` analog zu den
Zeitspalten (Jahr = `datetime.now().year`, unplausibler Monat/Tag → NULL mit
Log-Warnung). Backup: `pivot2db.py.bak_date20260812`.

- Backup der vier Rohspalten vor dem Umbau: Tabelle
  **`inverter_data_tou_bak20260812`** (207 671 Zeilen, id + timestamp + device_id
  + die vier Originalwerte).
- Umgestellte Zeilen: 198 946 (`TOU_Charge_Start`) bzw. 198 736 (`TOU_Charge_End`),
  alle auf 08:00:00 / 18:40:00. Zeilen vor dem 26.02.2026 haben NULL — damals wurden
  die TOU-Register noch nicht gepollt.
- Lücke: die vier Zeilen zwischen 17:41 und 17:47 des 12.08.2026 haben NULL. Sie
  entstanden zwischen Umrechnung und Spaltentausch; die Rohwerte fielen mit der alten
  Spalte weg. Bewusst nicht mit dem bekannten Wert aufgefüllt.

### `pivot2db.py` schreibt jetzt Uhrzeiten

Ergänzt um `TIME_COLUMNS` + `register_to_time()`; der Rohwert wird vor dem Insert
nach `'HH:MM:SS'` gewandelt, Unplausibles wird NULL (mit Log-Warnung). Zusätzlich ist
`insert_data()` angepasst, damit `sanitize_value()` diese Spalten nicht mehr als
numerisch behandelt und den String verwirft. Backup: `pivot2db.py.bak_time20260812`
(Pi und `/home/gh/python/`).

### Stolperstein beim ALTER: tmpfs `/tmp`

Der Tabellen-Rebuild scheiterte zunächst mit
`Got error 59 'Temp file write failure' from InnoDB` → im Error-Log:
`Write to file (merge) failed at offset 47185920 … Error number 28 'No space left on device'`.

Ursache: `tmpdir` von MariaDB ist `/tmp`, laut `/etc/fstab` eine **tmpfs mit 100 MB**,
davon 55 MB durch `fox2db_cron.log` belegt. Die Merge-Datei des Rebuilds einer
156-MB-Tabelle passt dort nicht. Was **nicht** funktioniert hat:

- `SET SESSION innodb_tmpdir='/var/tmp'` als User `gh` → braucht das FILE-Privileg
- `innodb_tmpdir` auf ein Verzeichnis im Datadir → von MariaDB abgelehnt
- `ALGORITHM=COPY` → landet ebenfalls im tmpdir

Was funktioniert hat: `/tmp` für die Dauer des ALTER vergrößern und danach
zurücksetzen (Skript `run_swap.sh`, Laufzeit des ALTER 35 s):

```bash
sudo mount -o remount,size=1G /tmp     # ... ALTER ...
sudo mount -o remount,size=100M /tmp   # fstab-Wert wiederhergestellt
```

Der Poller wird während des ganzen Fensters über `flock` auf `/tmp/sofar.lock`
ausgesperrt (`sofar.sh` überspringt dann seinen Durchlauf), damit keine Rohzahl in
eine bereits umgestellte TIME-Spalte fällt.

---

## 5. Auslesen ohne Buskonflikt

`sofar.sh` pollt jede Minute und hält dabei `/tmp/sofar.lock`. Jedes zusätzliche
Skript auf `/dev/ttyUSB32` muss dieselbe Sperre nehmen, sonst kollidieren die
Modbus-Frames:

```python
import fcntl
lock = open('/tmp/sofar.lock', 'a')
fcntl.flock(lock, fcntl.LOCK_EX)      # oder LOCK_NB + Retry-Schleife
```

Beispiel-Leseskript (nur lesend, mit Sperre und Dekodierung des kompletten Blocks):
`/home/pi/python/tou_read.py`.

Hinweis: `read.py` liest die TOU-Register einzeln (ein Modbus-Request je Register),
`tou_read.py` als einen Block von 32 Registern — beide liefern dieselben Rohwerte.
