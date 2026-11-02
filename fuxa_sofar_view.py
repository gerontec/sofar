#!/usr/bin/env python3
"""Add the Sofar HYD 20KTL-3PH (Lenggries) view to the venus.heissa.de FUXA project.
Memory-saving: no FUXA instance of its own - the view lives in the fuxa-venus project (127.0.0.1:1892) next to v_victron,
sofar.heissa.de opens it via /?viewName=Sofar (FUXA HomeComponent start view by name).
WebAPI device polling sofar_lenggries_api.php (one tag per value, 5 min snapshots of wagodb.inverter_data device_id 1).
The EPS (off-grid / backup) output feeds the top floor (Dachgeschoss).
Same widget formats as fuxa_victron_project.py / fuxa_ww_project.py.
usage: python3 fuxa_victron_project.py victron_api.json | python3 fuxa_sofar_view.py sofar_api.json > /tmp/venus_project.json"""
import json, sys
from html import escape

API, DEV, VIEW_ID, VIEW_NAME = 'https://heissa.de/web1/sofar_lenggries_api.php', 'd_sofar_api', 'v_sofar', 'Sofar'
FG, MUTED, STROKE = '#e5e7eb', '#9ca3af', '#cbd5e1'
GREEN, ORANGE, BLUE, GREY, PURPLE, RED, YELLOW = '#22c55e', '#f59e0b', '#60a5fa', '#6b7280', '#a78bfa', '#ef4444', '#facc15'
SCALE = 1.6
W, H = 1460, 800

# ---- tags: one per sofar_lenggries_api.php value ------------------------------------------------
api = json.load(open(sys.argv[1]))
tags = {}
for k, e in api['values'].items():
    typ = ('boolean' if isinstance(e['value'], bool) or e['unit'] == 'bool'
           else 'string' if isinstance(e['value'], str) or e['unit'] == 'text' else 'number')
    tags['s_' + k] = {'id': 's_' + k, 'name': k, 'label': k, 'type': typ, 'address': f'values:{k}:value',
                      'description': e['description'], 'daq': {'enabled': False, 'interval': 60, 'changed': True}}
tags['s_timestamp'] = {'id': 's_timestamp', 'name': 'timestamp', 'label': 'timestamp', 'type': 'string',
                       'address': 'timestamp', 'daq': {'enabled': False, 'interval': 60, 'changed': True}}

svg, items, n = [], {}, [0]


def nid(p='sof_'):
    n[0] += 1
    return f'{p}{n[0]:04d}'


def text(x, y, s, size=15, weight='normal', anchor='start', fill=FG):
    svg.append(f'<text id="{nid()}" x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" '
               f'font-family="sans-serif" fill="{fill}" stroke-width="0" xml:space="preserve">{escape(s, quote=False)}</text>')


def value(x, y, key, unit='', digits=1, size=17, weight='bold', anchor='start', fill=FG):
    assert 's_' + key in tags, key
    gid, tid = nid('SVAL_'), nid('SVAL_')
    svg.append(f'<g id="{gid}" type="svg-ext-value" fill="{fill}" font-size="{size}" font-family="sans-serif" text-anchor="{anchor}" stroke-width="0">'
               f'<text id="{tid}" x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" font-family="sans-serif" '
               f'fill="{fill}" stroke-width="0" xml:space="preserve">–</text></g>')
    items[gid] = {'id': gid, 'type': 'svg-ext-value', 'name': key, 'label': 'Value',
                  'property': {'variableId': 's_' + key, 'variableSrc': DEV, 'events': [], 'actions': [],
                               'ranges': [{'type': 1, 'text': unit, 'fractionDigits': digits}]}}


def shape(tag, attrs, key=None, ranges=None):
    sid = nid()
    a = ' '.join(f'{k}="{v}"' for k, v in attrs.items())
    svg.append(f'<{tag} id="{sid}" {a}/>')
    if key:
        assert 's_' + key in tags, key
        items[sid] = {'id': sid, 'type': f'svg-ext-shapes-{tag}', 'name': key, 'label': 'Shapes',
                      'property': {'variableId': 's_' + key, 'variableSrc': DEV, 'events': [], 'actions': [], 'ranges': ranges}}
    return sid


def box(x, y, w, h, fill, rx=12, key=None, ranges=None):
    shape('rect', {'x': x, 'y': y, 'width': w, 'height': h, 'rx': rx, 'fill': fill, 'stroke': STROKE, 'stroke-width': 1.2}, key, ranges)


def rng(lo, hi, c, prop):
    return {'type': 2, 'min': lo, 'max': hi, 'color': c if prop == 'color' else '', 'stroke': c if prop == 'stroke' else ''}


def flow_ranges(prop, neg, zero, pos, dead):
    """colour by sign: value < -dead -> neg, |value| <= dead -> zero, > dead -> pos (prop 'stroke' or 'color' = fill)"""
    return [rng(-1e6, -dead, neg, prop), rng(-dead, dead, zero, prop), rng(dead, 1e6, pos, prop)]


def on_off(off, on, prop='color'):
    """ranges for a 0/1 value: off below 0.5, on from 0.5"""
    return [rng(-1, 0.5, off, prop), rng(0.5, 2, on, prop)]


def line(d, key, neg, zero, pos, dead, width=7):
    shape('path', {'d': d, 'fill': 'none', 'stroke': zero, 'stroke-width': width, 'stroke-linecap': 'round', 'stroke-linejoin': 'round'},
          key, flow_ranges('stroke', neg, zero, pos, dead))


def dyn_text(x, y, s, key, ranges, size=13, weight='bold', anchor='start', fill=FG):
    """text whose fill follows the value of key ('none' hides it)"""
    sid = nid()
    svg.append(f'<text id="{sid}" x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" '
               f'font-family="sans-serif" fill="{fill}" stroke-width="0" xml:space="preserve">{escape(s, quote=False)}</text>')
    items[sid] = {'id': sid, 'type': 'svg-ext-shapes-text', 'name': key, 'label': 'Shapes',
                  'property': {'variableId': 's_' + key, 'variableSrc': DEV, 'events': [], 'actions': [], 'ranges': ranges}}


def soc_bar(x, y, key, seg_w=26, seg_h=22):
    """10 segments, segment i lights from SoC >= 10*i + 5; the lowest two orange"""
    for i in range(10):
        col = GREEN if i >= 2 else ORANGE
        shape('rect', {'x': x + i * (seg_w + 4), 'y': y, 'width': seg_w, 'height': seg_h, 'rx': 3, 'fill': '#1f2937',
                       'stroke': STROKE, 'stroke-width': 0.8},
              key, [rng(-1, 10 * i + 5, '#1f2937', 'color'), rng(10 * i + 5, 1000, col, 'color')])


def click_tile(x, y, w, h, name, event):
    sid = shape('rect', {'x': x, 'y': y, 'width': w, 'height': h, 'rx': 6, 'fill': '#ffffff', 'fill-opacity': 0.001, 'stroke': 'none'})
    items[sid] = {'id': sid, 'type': 'svg-ext-shapes-rect', 'name': name, 'label': 'Shapes',
                  'property': {'variableId': '', 'events': [dict(type='click', **event)], 'actions': [], 'ranges': []}}


def rows(x, y, entries, label_w, step=22, size=13, vsize=14):
    for i, (lab, key, unit, dig) in enumerate(entries):
        text(x, y + i * step, lab, size, fill=MUTED)
        value(x + label_w, y + i * step, key, unit, dig, vsize, anchor='end')


# ---- header ------------------------------------------------------------------------------------
text(30, 34, 'Sofar HYD 20KTL-3PH · Lenggries · EPS', 20, 'bold')
text(560, 34, 'as of', 13, fill=MUTED)
value(600, 34, 'timestamp', '', None, 13, 'normal', fill=MUTED)
text(790, 34, 'sofar_lenggries_api.php, snapshot every 5 min', 13, fill=MUTED)
text(30, 52, 'lines: green = power flowing in the arrow direction · orange = reverse · grey = idle · '
             'top floor box turns orange while the inverter runs off-grid', 11, fill=MUTED)

# ---- state (top left) ---------------------------------------------------------------------------
box(30, 70, 330, 250, '#1e1b3a')
text(48, 96, 'State', 15, 'bold')
value(340, 96, 'sys_state_text', '', None, 15, anchor='end')
dyn_text(48, 122, 'EPS output enabled', 'eps_on', on_off(MUTED, GREEN), 13)
dyn_text(210, 122, 'grid present', 'grid_ok', on_off(RED, GREEN), 13)
dyn_text(48, 146, 'OFF-GRID (EPS state)', 'island', on_off('none', ORANGE), 14, fill='none')
text(48, 172, 'faults', 13, fill=MUTED)
value(340, 172, 'fault_ids', '', None, 13, anchor='end')
shape('circle', {'cx': 120, 'cy': 168, 'r': 6, 'fill': GREY, 'stroke': 'none'}, 'fault_any', on_off(GREEN, RED))
rows(48, 198, [('residual current (RCMU)', 'gfci_ma', 'mA', 0), ('insulation PV', 'riso_kohm', 'kΩ', 0)], 292)
text(48, 246, 'DC component L1 / L2 / L3', 13, fill=MUTED)
for i, k in enumerate(('dci_l1_ma', 'dci_l2_ma', 'dci_l3_ma')):
    value(220 + i * 60, 268, k, 'mA', 0, 13, anchor='end')
text(48, 300, 'internal N-PE relay closes off-grid (0x0A06 bit0)', 11, fill=MUTED)

# ---- grid / Z1 (top middle) ---------------------------------------------------------------------
GRIDC = '#94a3b8'
box(400, 70, 330, 250, '#1f2937')
text(418, 96, 'Grid · Z1 (Sofar CT)', 15, 'bold')
value(712, 96, 'grid_f_hz', 'Hz', 2, 14, anchor='end')
text(418, 122, 'PCC', 13, fill=MUTED)
value(712, 122, 'pcc_kw', 'kW', 2, 20, anchor='end')
text(418, 140, '+ export · − import', 11, fill=MUTED)
for i, p in enumerate(('l1', 'l2', 'l3')):
    y = 168 + i * 24
    text(418, y, p.upper(), 13, fill=MUTED)
    value(560, y, f'grid_{p}_v', 'V', 1, 14, anchor='end')
    value(712, y, f'pcc_{p}_w', 'W', 0, 14, anchor='end')
rows(418, 250, [('bought today', 'purchase_today_kwh', 'kWh', 1), ('sold today', 'selling_today_kwh', 'kWh', 1),
                ('load behind Z1', 'load_sys_kw', 'kW', 2)], 294)

# ---- PV (top right) ------------------------------------------------------------------------------
box(770, 70, 300, 160, '#2a2410')
text(788, 96, 'PV', 15, 'bold', fill=YELLOW)
value(1052, 96, 'pv_kw', 'kW', 2, 20, anchor='end')
rows(788, 126, [('string 1', 'pv1_kw', 'kW', 2), ('string 2', 'pv2_kw', 'kW', 2), ('today', 'pv_today_kwh', 'kWh', 1)], 264)

# ---- navigation to the Victron view (top far right) -------------------------------------------
box(1110, 70, 320, 70, '#16263a', 8)
text(1128, 98, 'Victron MultiPlus ▸', 15, 'bold', fill=BLUE)
text(1128, 120, 'venus.heissa.de', 12, fill=MUTED)
click_tile(1110, 70, 320, 70, 'nav_victron', {'action': 'onOpenTab', 'actparam': 'https://venus.heissa.de/', 'actoptions': {'newTab': False}})

# ---- inverter (centre) ----------------------------------------------------------------------------
IX, IY, IW, IH = 520, 380, 420, 200
box(IX, IY, IW, IH, '#16263a')
text(IX + 18, IY + 28, 'Sofar HYD 20KTL-3PH', 16, 'bold')
text(IX + 18, IY + 48, 'hybrid inverter · Modbus RS485 → kellertreppe read.py', 11, fill=MUTED)
text(IX + 18, IY + 80, 'AC output (grid port)', 13, fill=MUTED)
value(IX + IW - 18, IY + 80, 'output_kw', 'kW', 2, 20, anchor='end')
for i, p in enumerate(('l1', 'l2', 'l3')):
    text(IX + 18 + i * 130, IY + 112, p.upper(), 13, fill=MUTED)
    value(IX + 120 + i * 130, IY + 112, f'output_{p}_w', 'W', 0, 14, anchor='end')
rows(IX + 18, IY + 146, [('EPS switch 0x1029 (1 = on)', 'eps_control', '', 0), ('run state 0x0404', 'sys_state', '', 0)], IW - 36)

# flow lines: PV -> inverter, grid <-> inverter, inverter <-> battery, inverter -> top floor
line(f'M920,230 L920,{IY}', 'pv_kw', ORANGE, GREY, GREEN, 0.02)                  # PV down into the inverter
line(f'M565,320 L565,{IY}', 'pcc_kw', GREEN, GREY, ORANGE, 0.05)                # + export: up (orange = to the grid), import green
text(575, 356, 'grid port', 12, fill=MUTED)
line(f'M{IX + 100},{IY + IH} L{IX + 100},640', 'bat_kw', ORANGE, GREY, GREEN, 0.02)  # + charging: down into the battery
line(f'M{IX + IW},{IY + 100} L1060,{IY + 100}', 'eps_kw', ORANGE, GREY, PURPLE, 0.005)
text(IX + IW + 12, IY + 90, 'EPS port', 12, fill=PURPLE)

# ---- battery (bottom left) ----------------------------------------------------------------------
BX, BY = 30, 640
box(BX, BY, 980, 100, '#0f1d33')
text(BX + 18, BY + 26, 'Battery (Sofar Bat1)', 15, 'bold', fill=BLUE)
value(BX + 18, BY + 72, 'bat_soc', '%', 0, 34)
soc_bar(BX + 120, BY + 46, 'bat_soc', 20, 20)
rows(BX + 450, BY + 30, [('power (+ charging)', 'bat_kw', 'kW', 2), ('SOH', 'bat_soh', '%', 0)], 220, 24)
rows(BX + 720, BY + 30, [('charged today', 'bat_charge_today_kwh', 'kWh', 1), ('discharged today', 'bat_discharge_today_kwh', 'kWh', 1)], 240, 24)

# ---- top floor (EPS output, right): orange box while off-grid ------------------------------------
TX, TY, TW, TH = 1060, 300, 370, 320
box(TX, TY, TW, TH, '#251a33', 12, 'island', on_off('#251a33', '#3a2a10'))
text(TX + 18, TY + 28, 'Top floor (attic)', 16, 'bold', fill=PURPLE)
text(TX + 18, TY + 48, 'EPS (backup) output of the Sofar, off-grid from PV / battery', 11, fill=MUTED)
text(TX + 18, TY + 80, 'EPS power', 13, fill=MUTED)
value(TX + TW - 18, TY + 80, 'eps_kw', 'kW', 2, 22, anchor='end')
text(TX + 110, TY + 108, 'W', 11, 'normal', 'end', MUTED)
text(TX + 220, TY + 108, 'V', 11, 'normal', 'end', MUTED)
text(TX + TW - 18, TY + 108, 'A', 11, 'normal', 'end', MUTED)
for i, p in enumerate(('l1', 'l2', 'l3')):
    y = TY + 132 + i * 24
    text(TX + 18, y, p.upper(), 13, fill=MUTED)
    value(TX + 110, y, f'eps_{p}_w', 'W', 0, 14, anchor='end')
    value(TX + 220, y, f'eps_{p}_v', 'V', 1, 14, anchor='end')
    value(TX + TW - 18, y, f'eps_{p}_a', 'A', 2, 14, anchor='end')
rows(TX + 18, TY + 220, [('frequency', 'eps_f_hz', 'Hz', 2), ('max today', 'eps_max_today_kw', 'kW', 2),
                         ('off-grid today', 'island_min_today', 'min', 0), ('no grid today', 'no_grid_min_today', 'min', 0)],
     TW - 36, 22)

# ---- view, appended to the venus project from stdin ---------------------------------------------
svgcontent = (f'<svg width="{W * SCALE}" height="{H * SCALE}" xmlns="http://www.w3.org/2000/svg" xmlns:svg="http://www.w3.org/2000/svg" '
              'xmlns:xlink="http://www.w3.org/1999/xlink">'
              f'<g><title>Layer 1</title><g id="svg_shift" transform="scale({SCALE})">' + ''.join(svg) + '</g></g></svg>')
view = {'id': VIEW_ID, 'name': VIEW_NAME, 'profile': {'width': W * SCALE, 'height': H * SCALE, 'bkcolor': '#000000ff', 'margin': 0},
        'items': items, 'variables': {}, 'svgcontent': svgcontent, 'type': 'svg'}
project = json.load(sys.stdin)
project['devices'][DEV] = {'id': DEV, 'name': 'sofar_lenggries_api', 'type': 'WebAPI', 'enabled': True, 'polling': 60000,
                           'property': {'address': API, 'method': 'GET', 'format': 'JSON'}, 'tags': tags}
project['hmi']['views'] = [v for v in project['hmi']['views'] if v['id'] != VIEW_ID] + [view]
json.dump(project, sys.stdout, ensure_ascii=False)
