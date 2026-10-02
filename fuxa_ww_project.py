#!/usr/bin/env python3
"""Generate a FUXA project that ports ww_hydraulik.php: WebAPI device polling ww_api.php,
one tag per value, and the hydraulic diagram as a FUXA view (values + temperature colours via ranges)."""
import json, math, sys

API = 'https://heissa.de/web1/ww_api.php'
DEV = 'd_ww_api'

FG, MUTED, STROKE = '#e5e7eb', '#9ca3af', '#cbd5e1'
HP, TANK, BOILER, ZEN, SOL, NOTE = '#16263a', '#3a2a1c', '#2a1a1c', '#15281a', '#3a3110', '#2a260e'

# colours like manufacturer schematics: supply (VL) red, return (RL) magenta, cold water green, DHW orange;
# the temperature (plant range 8..70 degC) sets the lightness: dark = cold, light = hot
T_MIN, T_MAX = 8, 70
HUE = {'vl': 0, 'rl': 300, 'dhw': 30, 'cold': 130}


def trgb(t, role='vl'):
    import colorsys
    f = min(max((t - T_MIN) / (T_MAX - T_MIN), 0.0), 1.0)
    return tuple(round(v * 255) for v in colorsys.hls_to_rgb(HUE[role] / 360, 0.32 + 0.43 * f, 1.0))


def hexc(c):
    return '#%02x%02x%02x' % c


def temp_ranges(kind, role='vl'):
    """1 K steps T_MIN..T_MAX; kind 'stroke' for pipes, 'fill' (darkened x0.38) for tanks."""
    out = []
    edges = [-50] + list(range(T_MIN, T_MAX + 1)) + [200]
    for lo, hi in zip(edges, edges[1:]):
        c = trgb(min(max(lo + 0.5, T_MIN), T_MAX), role)
        if kind == 'fill':
            c = tuple(round(v * 0.38) for v in c)
        out.append({'type': 2, 'min': lo, 'max': hi, 'color': hexc(c) if kind == 'fill' else '',
                    'stroke': hexc(c) if kind == 'stroke' else ''})
    return out


# ---- tags: one per ww_api.php value ------------------------------------------------------------
api = json.load(open(sys.argv[1])) if len(sys.argv) > 1 else None
keys = list(api['values']) if api else []
tags = {}
for k in keys:
    v = api['values'][k]['value']
    typ = 'boolean' if isinstance(v, bool) else 'number'
    tags['t_' + k] = {'id': 't_' + k, 'name': k, 'label': k, 'type': typ, 'address': f'values:{k}:value',
                      'description': api['values'][k]['description'], 'daq': {'enabled': False, 'interval': 60, 'changed': True}}
tags['t_timestamp'] = {'id': 't_timestamp', 'name': 'timestamp', 'label': 'timestamp', 'type': 'string', 'address': 'timestamp',
                       'daq': {'enabled': False, 'interval': 60, 'changed': True}}

# ---- SVG ----------------------------------------------------------------------------------------
svg, svg_top, items = [], [], {}
IMG = 'https://heissa.de/web1/fuxa_img/'
SHIFT_X, SHIFT_Y = -20, -60
n = [0]


def nid(p='svg_'):
    n[0] += 1
    return f'{p}{n[0]:04d}'


def text(x, y, s, size=15, weight='normal', anchor='start', fill=FG):
    svg.append(f'<text id="{nid()}" x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" '
               f'font-family="sans-serif" fill="{fill}" stroke-width="0" xml:space="preserve">{s}</text>')


def value(x, y, key, unit='', digits=1, size=17, weight='bold', anchor='start', fill=FG):
    gid, tid = nid('VAL_'), nid('VAL_')
    svg.append(f'<g id="{gid}" type="svg-ext-value" fill="{fill}" font-size="{size}" font-family="sans-serif" text-anchor="{anchor}" stroke-width="0">'
               f'<text id="{tid}" x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" font-family="sans-serif" '
               f'fill="{fill}" stroke-width="0" xml:space="preserve">–</text></g>')
    items[gid] = {'id': gid, 'type': 'svg-ext-value', 'name': key, 'label': 'Value',
                  'property': {'variableId': 't_' + key, 'variableSrc': DEV, 'events': [], 'actions': [],
                               'ranges': [{'type': 1, 'text': unit, 'fractionDigits': digits}]}}


def shape(tag, attrs, key=None, ranges=None, extra=None):
    sid = nid()
    a = ' '.join(f'{k}="{v}"' for k, v in attrs.items())
    svg.append(f'<{tag} id="{sid}" {a}/>')
    if key:
        items[sid] = {'id': sid, 'type': f'svg-ext-shapes-{tag}', 'name': key, 'label': 'Shapes',
                      'property': {'variableId': 't_' + key, 'variableSrc': DEV, 'events': [], 'actions': [], 'ranges': ranges}}
    return sid


def pipe(d, key=None, width=6, color=MUTED, arrow=True, role='vl'):
    attrs = {'d': d, 'fill': 'none', 'stroke': color, 'stroke-width': width, 'stroke-linecap': 'round', 'stroke-linejoin': 'round'}
    if arrow:
        attrs['marker-end'] = 'url(#arr)'
    shape('path', attrs, key, temp_ranges('stroke', role) if key else None)


def box(x, y, w, h, fill, rx=14):
    shape('rect', {'x': x, 'y': y, 'width': w, 'height': h, 'rx': rx, 'fill': fill, 'stroke': STROKE, 'stroke-width': 1.2})


def coil(xc, y0, y1, hw, turns):
    p = []
    for i in range(241):
        f = i / 240
        p.append(f'{round(xc + hw * math.sin(f * turns * 2 * math.pi), 1)},{round(y0 + (y1 - y0) * f, 1)}')
    return 'M' + ' L'.join(p)


def image(x, y, w, h, base_url, key, states):
    """FUXA image widget; states = [(min, max, url)] as loadImage actions. Unmatched values show base_url
    (needs the loadImage fallback of gerontec/FUXA feature/load-image-default). Placed outside the shift group."""
    gid, did = nid('OXC_'), nid('OXC_')
    x, y = x + SHIFT_X, y + SHIFT_Y
    svg_top.append(f'<g id="{gid}" type="svg-ext-own_ctrl-image" style="pointer-events:none" stroke-width="0">'
                   f'<rect x="{x}" y="{y}" width="{w}" height="{h}" stroke-width="0" fill="none" id="{nid()}"/>'
                   f'<foreignObject x="{x}" y="{y}" width="{w}" height="{h}" id="H-{did}">'
                   f'<div xmlns="http://www.w3.org/1999/xhtml" id="D-{did}" style="width:100%;height:100%;"></div></foreignObject></g>')
    items[gid] = {'id': gid, 'type': 'svg-ext-own_ctrl-image', 'name': key, 'label': 'HtmlImage',
                  'property': {'address': base_url, 'variableId': '', 'events': [],
                               'actions': [{'variableId': 't_' + key, 'type': 'loadImage', 'range': {'min': lo, 'max': hi},
                                            'options': {'url': url}} for lo, hi, url in states]}}


def tank(xc, ytop, ybot, rx, ry, fill, key, role='vl'):
    """cylinder: body + top ellipse coloured by key, static bottom arc"""
    tr = temp_ranges('fill', role)
    shape('rect', {'x': xc - rx, 'y': ytop, 'width': 2 * rx, 'height': ybot - ytop, 'fill': fill, 'stroke': STROKE, 'stroke-width': 1.2}, key, tr)
    shape('ellipse', {'cx': xc, 'cy': ytop, 'rx': rx, 'ry': ry, 'fill': fill, 'stroke': STROKE, 'stroke-width': 1.2}, key, tr)
    shape('path', {'d': f'M{xc - rx},{ybot} a{rx},{ry} 0 0 0 {2 * rx},0', 'fill': fill, 'stroke': STROKE, 'stroke-width': 1.2})


# header line (replaces the HTML header of ww_hydraulik.php)
text(30, 86, 'WW-Hydraulik live (FUXA)', 17, 'bold')
text(260, 86, 'Stand', 13, fill=MUTED)
value(300, 86, 'timestamp', '', None, 13, 'normal', fill=MUTED)

# heat pump: full-height box with the main parts of the R290 monobloc (refrigerant circuit, heating mode)
box(30, 100, 210, 635, HP)
text(118, 128, 'WP 22kW R290', 16, 'bold', 'middle')
image(196, 104, 36, 36, IMG + 'hp_normal.svg', 'defrost_active', [(1, 1, IMG + 'hp_defrost.svg')])
text(130, 152, 'außen', 13, anchor='end', fill=MUTED)
value(136, 152, 'outdoor_temp', '°C', 1, 13, 'normal', fill=MUTED)


def refrig(d, color, arrow=False):
    """refrigerant line: coloured while the compressor runs, grey when off"""
    attrs = {'d': d, 'fill': 'none', 'stroke': MUTED, 'stroke-width': 3, 'stroke-linejoin': 'round'}
    if arrow:
        attrs['marker-end'] = 'url(#arr)'
    shape('path', attrs, 'compressor_freq', [{'type': 2, 'min': -1, 'max': 0.5, 'color': '', 'stroke': MUTED},
                                             {'type': 2, 'min': 0.5, 'max': 500, 'color': '', 'stroke': color}])


HOTGAS, LIQUID, SUCTION = '#ef4444', '#f59e0b', '#38bdf8'
# evaporator (finned coil) with fan
shape('rect', {'x': 50, 'y': 168, 'width': 170, 'height': 76, 'rx': 4, 'fill': '#0f1c2b', 'stroke': STROKE, 'stroke-width': 1.2})
shape('path', {'d': ' '.join(f'M{x},172 L{x},240' for x in range(58, 220, 8)), 'stroke': '#3b5068', 'stroke-width': 1.5, 'fill': 'none'})
shape('circle', {'cx': 135, 'cy': 206, 'r': 30, 'fill': '#16263a', 'stroke': STROKE, 'stroke-width': 1.2})
shape('path', {'d': ''.join(f'M135,206 Q{round(135 + 26 * math.cos(a + 0.6), 1)},{round(206 + 26 * math.sin(a + 0.6), 1)} '
                            f'{round(135 + 26 * math.cos(a), 1)},{round(206 + 26 * math.sin(a), 1)} Z '
                            for a in (0, 2.094, 4.189)), 'fill': MUTED, 'stroke': 'none'})
shape('circle', {'cx': 135, 'cy': 206, 'r': 4, 'fill': STROKE})
text(165, 262, 'Verdampfer + Lüfter', 12, anchor='middle', fill=MUTED)

# 4-way valve (reverses the circuit for defrost)
shape('rect', {'x': 80, 'y': 290, 'width': 40, 'height': 26, 'rx': 3, 'fill': '#0f1c2b', 'stroke': STROKE, 'stroke-width': 1.2})
text(100, 307, '4WV', 11, 'bold', 'middle')

# plate heat exchanger = condenser
shape('rect', {'x': 155, 'y': 295, 'width': 45, 'height': 120, 'rx': 3, 'fill': '#0f1c2b', 'stroke': STROKE, 'stroke-width': 1.2})
shape('path', {'d': ' '.join(f'M159,{y} L196,{y}' for y in range(303, 410, 8)), 'stroke': '#3b5068', 'stroke-width': 1.5, 'fill': 'none'})
text(178, 433, 'Platten-WT', 11, anchor='middle', fill=MUTED)
text(178, 447, 'Kondensator', 11, anchor='middle', fill=MUTED)

# compressor
shape('circle', {'cx': 100, 'cy': 500, 'r': 30, 'fill': '#0f1c2b', 'stroke': STROKE, 'stroke-width': 1.5})
shape('path', {'d': 'M80,478 L120,490 L120,510 L80,522', 'fill': 'none', 'stroke': STROKE, 'stroke-width': 1.5})
text(100, 550, 'Verdichter', 13, anchor='middle', fill=MUTED)
value(100, 574, 'compressor_freq', 'Hz', None, anchor='middle')
value(100, 596, 'heat_pump_electric_power', 'kW el', 1, 15, 'normal', 'middle')

# expansion valve (bow tie)
shape('path', {'d': 'M37,418 L53,418 L37,442 L53,442 Z', 'fill': '#0f1c2b', 'stroke': STROKE, 'stroke-width': 1.5})
text(58, 435, 'EEV', 12, fill=MUTED)

# refrigerant circuit: compressor -> 4WV -> condenser -> EEV -> evaporator -> 4WV -> compressor
refrig('M112,472 L112,316', HOTGAS)
refrig('M120,303 L155,303', HOTGAS, True)
refrig('M178,415 L178,422 L225,422 L225,612 L45,612 L45,442', LIQUID)
refrig('M45,418 L45,256 L60,256 L60,244', SUCTION, True)
refrig('M100,244 L100,290', SUCTION)
refrig('M88,316 L88,472', SUCTION, True)
text(178, 290, 'Heißgas', 11, anchor='middle', fill=MUTED)

# water side with internal circulation pump
pipe('M200,325 L240,325', 'heat_pump_outlet_temp', arrow=False)
pipe('M240,385 L200,385', 'heat_pump_inlet_temp', arrow=False, role='rl')
shape('circle', {'cx': 222, 'cy': 385, 'r': 9, 'fill': '#0f1c2b', 'stroke': STROKE, 'stroke-width': 1.5})
shape('path', {'d': 'M226,385 L219,380 L219,390 Z', 'fill': STROKE})

text(135, 648, 'Modus', 13, anchor='end', fill=MUTED)
value(141, 648, 'heat_pump_mode', '(3 = DHW)', None, 13, 'normal', fill=MUTED)
text(135, 670, 'Kältemittel R290 (Propan)', 13, anchor='middle', fill=MUTED)
text(135, 692, 'Durchfluss WP ungemessen', 13, anchor='middle', fill=MUTED)

# power supply: CEE 400 V 3~ socket (5 pins L1 L2 L3 N + PE at 6 o'clock)
shape('path', {'d': 'M296,160 L240,160', 'fill': 'none', 'stroke': '#78716c', 'stroke-width': 5, 'stroke-linecap': 'round'})
shape('rect', {'x': 296, 'y': 128, 'width': 64, 'height': 64, 'rx': 8, 'fill': '#7f1d1d', 'stroke': STROKE, 'stroke-width': 1.2})
shape('circle', {'cx': 328, 'cy': 160, 'r': 24, 'fill': '#dc2626', 'stroke': '#fca5a5', 'stroke-width': 1.2})
for i, a in enumerate((90, 162, 234, 306, 18)):
    r = 15
    shape('circle', {'cx': round(328 + r * math.cos(math.radians(a)), 1), 'cy': round(160 + r * math.sin(math.radians(a)), 1),
                     'r': 4.5 if i == 0 else 3.2, 'fill': '#1f0a0a'})
text(372, 154, 'CEE 400 V', 14, 'bold')
text(372, 172, '3~ L1 L2 L3 N PE', 11, fill=MUTED)

# Modbus RTU: RS485 A/B from the heat pump (slave 1) and the SDM72D (slave 7) to the Raspi kellertreppe
BUS_A, BUS_B = '#fde047', '#93c5fd'
shape('rect', {'x': 212, 'y': 700, 'width': 26, 'height': 22, 'rx': 2, 'fill': '#0f1c2b', 'stroke': STROKE, 'stroke-width': 1.2})
text(208, 712, 'ID 1', 11, anchor='end', fill=MUTED)
text(222, 709, 'A', 8, 'bold', 'middle', BUS_A)
text(230, 719, 'B', 8, 'bold', 'middle', BUS_B)
shape('path', {'d': 'M238,706 L262,706 M250,706 L250,572 L262,572', 'fill': 'none', 'stroke': BUS_A, 'stroke-width': 2})
shape('path', {'d': 'M238,714 L262,714 M254,714 L254,582 L262,582', 'fill': 'none', 'stroke': BUS_B, 'stroke-width': 2})
shape('circle', {'cx': 250, 'cy': 706, 'r': 2.5, 'fill': BUS_A})
shape('circle', {'cx': 254, 'cy': 714, 'r': 2.5, 'fill': BUS_B})
text(322, 548, 'RS485 Modbus RTU', 11, anchor='middle', fill=MUTED)
box(262, 558, 120, 40, '#1c1c2a', 6)
text(322, 575, 'SDM72D', 13, 'bold', 'middle')
text(322, 591, 'ID 7', 11, anchor='middle', fill=MUTED)
box(262, 626, 120, 98, '#1c1c2a', 6)
text(322, 645, 'Raspi', 14, 'bold', 'middle')
text(322, 661, 'kellertreppe', 12, anchor='middle', fill=MUTED)
text(322, 678, '192.168.178.218', 12, anchor='middle')
text(322, 696, 'ttyUSB33 (Prolific)', 11, anchor='middle', fill=MUTED)
text(322, 712, 'Poll 60 s → wagodb', 11, anchor='middle', fill=MUTED)

# buffer
tank(395, 240, 510, 75, 18, TANK, 'buffer_tank_temp')
text(395, 350, 'Puffer', 16, 'bold', 'middle')
value(395, 380, 'buffer_tank_temp', '°C', 1, anchor='middle')
text(395, 404, 'WP-Fühler', 13, anchor='middle', fill=MUTED)

# HP loop
pipe('M240,325 L320,325', 'heat_pump_outlet_temp')
pipe('M320,385 L240,385', 'heat_pump_inlet_temp', role='rl')
value(280, 312, 'heat_pump_outlet_temp', '°C', 1, 15, 'normal', 'middle')
value(280, 410, 'heat_pump_inlet_temp', '°C', 1, 15, 'normal', 'middle')

# DHW boiler with coils
tank(720, 190, 650, 100, 20, BOILER, 'dhw_temp', 'dhw')
text(852, 215, 'WW-Boiler', 16, 'bold')
shape('path', {'d': coil(720, 245, 445, 70, 6), 'fill': 'none', 'stroke': MUTED, 'stroke-width': 4}, 'buffer_tank_temp', temp_ranges('stroke'))
shape('path', {'d': coil(720, 575, 625, 55, 2), 'fill': 'none', 'stroke': MUTED, 'stroke-width': 4}, 'solar_temp', temp_ranges('stroke'))
text(720, 475, 'Puffer-Wendel', 13, anchor='middle', fill=MUTED)
text(720, 648, 'Solar-Wendel', 13, anchor='middle', fill=MUTED)

# tap and cold water
pipe('M785,175 L785,95', 'dhw_temp', 5, role='dhw')
text(795, 110, 'WW-Zapfung', 13, fill=MUTED)
pipe('M560,700 L640,700 L640,662', None, 9, MUTED, False)
pipe('M560,700 L640,700 L640,655', None, 5, hexc(trgb(40, 'cold')))
text(552, 722, '~8 °C', 13, anchor='end', fill=MUTED)
text(552, 704, 'Kaltwasser', 13, anchor='end', fill=MUTED)

# sensors on boiler
shape('line', {'x1': 820, 'y1': 265, 'x2': 845, 'y2': 265, 'stroke': STROKE})
text(852, 260, 'WW gezapft')
value(852, 284, 'dhw_temp', '°C', 1)
value(852, 304, 'dhw_temp_rate', 'K/h (30 min)', 1, 13, 'normal', fill=MUTED)
shape('circle', {'cx': 862, 'cy': 560, 'r': 8, 'fill': MUTED}, 'dhw_pump_on',
      [{'type': 2, 'min': 0, 'max': 0, 'color': MUTED, 'stroke': ''}, {'type': 2, 'min': 1, 'max': 1, 'color': '#22a35a', 'stroke': ''}])
text(876, 565, 'WW-Pumpe', 13, fill=MUTED)

# primary series loop
pipe('M470,290 L560,290 L560,130 L720,130 L720,245', 'zenner_supply_temp')
shape('circle', {'cx': 640, 'cy': 130, 'r': 7, 'fill': ZEN, 'stroke': STROKE})
text(634, 114, 'Zenner-VL', anchor='end')
value(640, 114, 'zenner_supply_temp', '°C · DN25', None, 15, 'normal')
pipe('M720,445 L720,460 M720,460 L1240,460 L1240,335 L1290,335', 'heating_supply_temp')
shape('rect', {'x': 845, 'y': 432, 'width': 60, 'height': 56, 'rx': 6, 'fill': BOILER, 'stroke': STROKE, 'stroke-width': 1.2},
      'coil_outlet_temp', temp_ranges('fill'))
shape('line', {'x1': 875, 'y1': 488, 'x2': 875, 'y2': 505, 'stroke': STROKE})
shape('circle', {'cx': 875, 'cy': 505, 'r': 4, 'fill': STROKE})
text(875, 526, 'Kessel', anchor='middle')
value(875, 550, 'coil_outlet_temp', '°C', 1, anchor='middle')
shape('circle', {'cx': 985, 'cy': 460, 'r': 7, 'fill': ZEN, 'stroke': STROKE})
text(915, 444, 'VL nach Wendel')
value(1033, 444, 'heating_supply_temp', '°C (SPS)', 1, 15, 'normal')

# heating circuits
box(1290, 275, 180, 120, ZEN)
text(1380, 325, 'Heizkreise', 16, 'bold', 'middle')
text(1380, 350, '3 Wohnungen', anchor='middle')
pipe('M1380,395 L1380,735 L395,735 L395,530', 'zenner_return_temp', role='rl')

# Zenner meter in the return
box(1290, 425, 180, 120, ZEN)
text(1380, 450, 'Zenner gesamt', 16, 'bold', 'middle')
text(1322, 474, 'VL', anchor='end')
value(1327, 474, 'zenner_supply_temp', '', None, 15, 'normal')
text(1392, 474, 'RL', anchor='end')
value(1397, 474, 'zenner_return_temp', '°C', None, 15, 'normal')
value(1380, 498, 'zenner_flow', 'l/min', 1, 15, 'normal', 'middle')
value(1380, 528, 'zenner_power', 'kW', 1, anchor='middle')
text(890, 725, 'RL', anchor='end')
value(896, 725, 'zenner_return_temp', '°C (Zenner)', None, 15, 'normal')
shape('circle', {'cx': 1380, 'cy': 600, 'r': 7, 'fill': ZEN, 'stroke': STROKE})
text(1394, 605, 'Zenner-RL', 13, fill=MUTED)

# solar
box(1080, 580, 230, 100, SOL)
text(1195, 610, 'Solarthermie', 16, 'bold', 'middle')
value(1195, 640, 'solar_temp', '°C', 1, anchor='middle')
text(1195, 665, 'temp_solar', 13, anchor='middle', fill=MUTED)
pipe('M1080,610 L930,610 L930,575 L720,575', 'solar_temp', 4)
pipe('M720,625 L910,625 L910,655 L1080,655', None, 4, hexc(trgb(30, 'rl')))

# result box
box(1000, 70, 470, 150, NOTE, 10)
text(1015, 98, 'WarmWasser Anteil', 16, 'bold')
value(1015, 130, 'power_dhw', 'kW', 1, 26)
value(1130, 130, 'power_dhw_share', '% vom Gesamt', None, 20, 'normal')
text(1015, 155, 'Zenner gesamt')
value(1124, 155, 'power_total', 'kW', 1, 15, 'normal')
text(1205, 155, '· Heizkreise')
value(1290, 155, 'power_heating', 'kW (Ø 10 min)', 1, 15, 'normal')
text(1015, 177, 'WW = Flow × 1,163 × (Zenner-VL − Kessel) · Puffer + Solar = WW + HK', 13, fill=MUTED)
text(1015, 199, 'Flow Ø 30 Tage', 13, fill=MUTED)
value(1112, 199, 'zenner_flow_avg30d', 'l/min', 1, 13, 'normal', fill=MUTED)

W, H = 1460, 690
svgcontent = (f'<svg width="{W}" height="{H}" xmlns="http://www.w3.org/2000/svg" xmlns:svg="http://www.w3.org/2000/svg">'
              '<defs><marker id="arr" viewBox="0 0 10 10" refX="7" refY="5" markerWidth="4" markerHeight="4" orient="auto-start-reverse">'
              '<path d="M0,0 L10,5 L0,10 z" fill="context-stroke"/></marker></defs>'
              '<g><title>Layer 1</title><g id="svg_shift" transform="translate(-20,-60)">' + ''.join(svg) + '</g>' + ''.join(svg_top) + '</g></svg>')

view = {'id': 'v_ww_hydraulik', 'name': 'WW-Hydraulik', 'profile': {'width': W, 'height': H, 'bkcolor': '#000000ff', 'margin': 0},
        'items': items, 'variables': {}, 'svgcontent': svgcontent, 'type': 'svg'}
project = {
    'version': '1.01', 'name': 'WW-Hydraulik',
    'server': {'id': '0', 'name': 'FUXA Server', 'type': 'FuxaServer', 'property': {}},
    'devices': {DEV: {'id': DEV, 'name': 'ww_api', 'type': 'WebAPI', 'enabled': True, 'polling': 60000,
                      'property': {'address': API, 'method': 'GET', 'format': 'JSON'}, 'tags': tags}},
    'hmi': {'views': [view], 'layout': {'start': view['id'], 'navigation': {'mode': 'void', 'type': 'inline', 'items': []},
                                        'header': {'title': 'WW-Hydraulik', 'bkcolor': '#000000', 'fontcolor': FG}, 'showdev': False,
                                        'zoom': 'autoresize', 'inputdialog': 'false', 'hidenavigation': True}},
    'charts': [], 'alarms': [], 'notifications': [], 'scripts': [], 'reports': [], 'texts': [], 'plugin': [],
}
json.dump(project, sys.stdout, ensure_ascii=False)
