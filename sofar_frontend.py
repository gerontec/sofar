#!/usr/bin/env python3
"""Sofar fox2db — Steuerungs-Frontend.

Self-contained Web-Dashboard für die Waveshare-ESP32-Steuerung:
  - liest Live-Zustand via MQTT (sofar/state, sofar/waveshare/status)
  - zeigt letzte Entscheidungen aus MariaDB (pv_decision_log)
  - sendet Steuerbefehle (Auto/Ladesperre an/aus, Hand-Relais)

Start:  python3 sofar_frontend.py        # http://<host>:8088/
Deps:   paho-mqtt, pymysql  (bereits vorhanden)
"""

import json, threading, time
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import paho.mqtt.client as mqtt
import pymysql

MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
DB = dict(host="192.168.178.218", user="gh", password="a12345", database="wagodb",
          connect_timeout=4)
HTTP_PORT = 8088

# ── Live-Zustand aus MQTT ────────────────────────────────────────────────────
_lock = threading.Lock()
_state = {"data": {}, "ts": 0}      # sofar/state   (ESP / dev)
_status = {"data": {}, "ts": 0}     # sofar/waveshare/status
_prod = {"data": {}, "ts": 0}       # fox2db/state  (Pi / prod)
_pub = None                          # MQTT-Client zum Publizieren


def _on_connect(c, u, f, rc, props=None):
    c.subscribe("sofar/state")
    c.subscribe("sofar/waveshare/status")
    c.subscribe("fox2db/state")


def _on_message(c, u, msg):
    try:
        d = json.loads(msg.payload.decode())
    except Exception:
        return
    with _lock:
        if msg.topic == "sofar/state":
            _state["data"] = d; _state["ts"] = time.time()
        elif msg.topic == "sofar/waveshare/status":
            _status["data"] = d; _status["ts"] = time.time()
        elif msg.topic == "fox2db/state":
            _prod["data"] = d; _prod["ts"] = time.time()


def mqtt_thread():
    global _pub
    c = mqtt.Client(client_id="sofar_frontend", clean_session=True)
    c.on_connect = _on_connect
    c.on_message = _on_message
    while True:
        try:
            c.connect(MQTT_HOST, MQTT_PORT, 30)
            _pub = c
            c.loop_forever()
        except Exception:
            time.sleep(5)


def publish(topic, payload):
    if _pub is None:
        return False
    try:
        _pub.publish(topic, payload, qos=0)
        return True
    except Exception:
        return False


def recent_decisions(limit=20):
    try:
        conn = pymysql.connect(cursorclass=pymysql.cursors.DictCursor, **DB)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts, state_from, state_to, decision, detail, pcc_w, soc, "
                "excess_w, dc_delta_w FROM pv_decision_log ORDER BY ts DESC LIMIT %s", (limit,))
            rows = cur.fetchall()
        conn.close()
        for r in rows:
            r["ts"] = r["ts"].strftime("%H:%M:%S")
            for k, v in r.items():
                if isinstance(v, Decimal):
                    r[k] = float(v)
        return rows
    except Exception as e:
        return [{"error": str(e)}]


# ── HTML ─────────────────────────────────────────────────────────────────────
PAGE = """<!doctype html><html lang=de><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Sofar fox2db Steuerung</title>
<style>
 :root{--bg:#0f1419;--card:#1a2129;--ac:#4aa3df;--ok:#3fb950;--warn:#d29922;--err:#f85149;--mut:#8b949e}
 *{box-sizing:border-box} body{margin:0;font:14px/1.4 system-ui,sans-serif;background:var(--bg);color:#e6edf3}
 header{padding:14px 18px;background:#161b22;border-bottom:1px solid #30363d;display:flex;gap:14px;align-items:center;flex-wrap:wrap}
 h1{font-size:18px;margin:0} .pill{padding:2px 8px;border-radius:10px;font-size:12px;font-weight:600}
 .on{background:rgba(63,185,80,.18);color:var(--ok)} .off{background:rgba(139,148,158,.18);color:var(--mut)}
 .live{margin-left:auto;font-size:12px;color:var(--mut)}
 main{padding:18px;max-width:1100px;margin:0 auto}
 .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;margin-bottom:18px}
 .card{background:var(--card);border:1px solid #30363d;border-radius:10px;padding:12px}
 .card .k{font-size:11px;color:var(--mut);text-transform:uppercase;letter-spacing:.04em}
 .card .v{font-size:24px;font-weight:700;margin-top:4px} .card .u{font-size:13px;color:var(--mut);font-weight:400}
 .trace{background:var(--card);border:1px solid #30363d;border-radius:10px;padding:10px 12px;font-family:ui-monospace,monospace;font-size:13px;color:#c9d1d9;margin-bottom:18px;word-break:break-word}
 .ctl{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:18px}
 button{background:#21262d;color:#e6edf3;border:1px solid #30363d;border-radius:7px;padding:8px 12px;cursor:pointer;font-size:13px}
 button:hover{border-color:var(--ac)} button.ok{border-color:var(--ok)} button.err{border-color:var(--err)}
 table{width:100%;border-collapse:collapse;font-size:13px} th,td{text-align:left;padding:5px 8px;border-bottom:1px solid #21262d}
 th{color:var(--mut);font-weight:600} td.num{text-align:right;font-variant-numeric:tabular-nums}
 .sec{font-size:12px;color:var(--mut);text-transform:uppercase;letter-spacing:.04em;margin:18px 0 8px}
 .guard{color:var(--warn)} .do4{color:var(--err)}
</style></head><body>
<header>
 <h1>⚡ Sofar fox2db</h1>
 <span id=auto class="pill off">AUTO</span>
 <span id=lade class="pill off">LADESPERRE</span>
 <span id=do4 class="pill off">DO4</span>
 <span class=live id=live>—</span>
</header>
<main>
 <div class=grid id=cards></div>
 <div class=trace id=trace>warte auf Daten…</div>

 <div class=sec>Vergleich Pi (prod) ↔ ESP (dev) — Shadow</div>
 <table><thead><tr><th>Größe</th><th>Pi · fox2db</th><th>ESP · waveshare</th><th></th></tr></thead>
   <tbody id=cmpb></tbody></table>

 <div class=sec>Steuerung</div>
 <div class=ctl>
   <button class=ok  onclick="cmd('auto',1)">Auto AN</button>
   <button class=err onclick="cmd('auto',0)">Auto AUS</button>
   <button onclick="cmd('ladesperre',1)">Ladesperre AN</button>
   <button onclick="cmd('ladesperre',0)">Ladesperre AUS</button>
   <button class=err onclick="cmd('all',0)">Alle Relais AUS</button>
 </div>
 <div class=ctl id=relays></div>

 <div class=sec>Board</div>
 <div class=trace id=board>—</div>

 <div class=sec>Letzte Entscheidungen</div>
 <table><thead><tr><th>Zeit</th><th>von→nach</th><th>Decision</th><th class=num>PCC</th>
   <th class=num>SOC2</th><th class=num>Excess</th><th class=num>dc_Δ</th><th>Detail</th></tr></thead>
   <tbody id=dec></tbody></table>
</main>
<script>
const $=s=>document.querySelector(s);
function pill(id,on){const e=$('#'+id);e.className='pill '+(on?'on':'off');}
function card(k,v,u=''){return `<div class=card><div class=k>${k}</div><div class=v>${v}<span class=u> ${u}</span></div></div>`}
async function cmd(c,v){await fetch(`/api/cmd?c=${c}&v=${v}`);setTimeout(refresh,300);}
function relayBtns(){let h='';for(let i=1;i<=6;i++){h+=`<button onclick="cmd('relay${i}',1)">CH${i} AN</button>`+
   `<button onclick="cmd('relay${i}',0)">CH${i} AUS</button>`;}$('#relays').innerHTML=h;}
async function refresh(){
 try{const r=await fetch('/api/state');const j=await r.json();const s=j.state||{},b=j.status||{};
  pill('auto', s.auto!==0 && j.age_s<200); pill('lade', s.ladesperre==1); pill('do4', s.do4==1);
  $('#live').textContent = j.age_s<999 ? `live · ${j.age_s}s` : 'keine Daten';
  $('#cards').innerHTML =
    card('State', s.state??'—')+card('PCC', s.pcc??'—','W')+card('SOC2', s.soc2??'—','%')+
    card('SOC1', s.soc1??'—','%')+card('EBox', s.ebox??'—','W')+card('Excess', s.excess??'—','W')+
    card('DC erwartet', s.dc_expected??'—','W')+card('DC Δ', s.dc_delta??'—','W');
  let tr=s.trace||'—'; tr=tr.replace(/GUARD:[A-Z_]+/g,m=>`<span class=guard>${m}</span>`);
  $('#trace').innerHTML=tr;
  $('#board').innerHTML = b.ip ? `IP ${b.ip}${b.ip6?' · '+b.ip6:''} · Heap ${(b.mem_free/1024|0)} KB · Up ${b.uptime}s` : 'kein Board-Status';
  // Vergleich Pi (prod) <-> ESP (dev)
  const p=j.prod||{}, dec=t=>(t||'').split('|')[0].split('(')[0].trim();
  const pAge=j.prod_age_s<999?` (${j.prod_age_s}s)`:' (—)';
  const rows=[['State',p.state,s.state,1],['Decision',dec(p.trace),dec(s.trace),1],
    ['Ladesperre',p.ladesperre,s.ladesperre,1],['PCC',p.pcc,s.pcc,0],['SOC2',p.soc_bat2,s.soc2,0]];
  $('#cmpb').innerHTML=rows.map(([k,a,b,strict])=>{
    const mm=strict&&a!=null&&b!=null&&String(a)!==String(b);
    const mark=!strict?'':(mm?'<span class=do4>≠</span>':'<span style=color:#3fb950>=</span>');
    return `<tr${mm?' style=background:rgba(248,81,73,.10)':''}><td>${k}${k==='State'?pAge:''}</td>`+
           `<td>${a??'—'}</td><td>${b??'—'}</td><td>${mark}</td></tr>`;}).join('');
 }catch(e){$('#live').textContent='Fehler';}
}
async function decisions(){
 try{const r=await fetch('/api/decisions');const rows=await r.json();
  $('#dec').innerHTML=rows.map(d=>d.error?`<tr><td colspan=8>${d.error}</td></tr>`:
   `<tr><td>${d.ts}</td><td>${d.state_from}→${d.state_to}</td><td>${d.decision}</td>
    <td class=num>${d.pcc_w}</td><td class=num>${d.soc}</td><td class=num>${d.excess_w}</td>
    <td class=num>${d.dc_delta_w}</td><td style=color:#8b949e;font-size:12px>${(d.detail||'').slice(0,60)}</td></tr>`).join('');
 }catch(e){}
}
relayBtns();refresh();decisions();setInterval(refresh,2000);setInterval(decisions,10000);
</script></body></html>"""


CMD_MAP = {
    "auto":       lambda v: ("sofar/auto",       '{"ENABLE":%d}' % v),
    "ladesperre": lambda v: ("sofar/ladesperre", '{"ENABLE":%d}' % v),
    "all":        lambda v: ("waveshare/relay/all", '{"V":%d}' % v),
}
for _i in range(1, 7):
    CMD_MAP["relay%d" % _i] = (lambda v, i=_i: ("waveshare/relay/%d" % i, '{"V":%d}' % v))


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json"):
        b = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/" or u.path == "/index.html":
            return self._send(200, PAGE, "text/html; charset=utf-8")
        if u.path == "/api/state":
            with _lock:
                age = int(time.time() - max(_state["ts"], 1)) if _state["ts"] else 999
                page = int(time.time() - max(_prod["ts"], 1)) if _prod["ts"] else 999
                out = {"state": _state["data"], "status": _status["data"],
                       "prod": _prod["data"], "age_s": age, "prod_age_s": page}
            return self._send(200, json.dumps(out))
        if u.path == "/api/decisions":
            return self._send(200, json.dumps(recent_decisions()))
        if u.path == "/api/cmd":
            q = parse_qs(u.query)
            c = q.get("c", [""])[0]; v = int(q.get("v", ["0"])[0])
            fn = CMD_MAP.get(c)
            if fn:
                topic, payload = fn(v)
                ok = publish(topic, payload)
                return self._send(200, json.dumps({"ok": ok, "topic": topic, "payload": payload}))
            return self._send(400, json.dumps({"error": "unknown cmd"}))
        self._send(404, json.dumps({"error": "not found"}))


def main():
    threading.Thread(target=mqtt_thread, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), H)
    print(f"Sofar-Frontend: http://0.0.0.0:{HTTP_PORT}/")
    srv.serve_forever()


if __name__ == "__main__":
    main()
