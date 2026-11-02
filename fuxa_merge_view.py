#!/usr/bin/env python3
"""Merge the views and devices of another FUXA project into a base project (memory-saving: one FUXA instance, several
entry URLs). Tag ids get a new prefix (both generators use t_), SVG element / item ids in the merged views get the same
prefix, so nothing collides with the base project. A device id that already exists in the base gets the prefix after d_
(d_sofar_api -> d_b_sofar_api), variableSrc / deviceId references follow. Layout, users, settings stay those of the base project.
Entry URL of a merged view: https://<host>/?viewName=<view name> (FUXA HomeComponent start view by name).
usage: python3 fuxa_victron_project.py victron.json | python3 fuxa_sofar_view.py sofar.json \\
         | python3 fuxa_merge_view.py ww_project.json w_ > /tmp/venus_project.json
  ww_project.json = output of fuxa_ww_project.py (gerontec/FUXA branch heissa) or GET /api/project of the old instance"""
import json, re, sys

base = json.load(sys.stdin)
other = json.load(open(sys.argv[1]))
prefix = sys.argv[2]
assert re.fullmatch(r'[a-z]+_', prefix), 'prefix like w_'

# tag ids: t_xxx -> w_xxx (any old prefix before the first _ is replaced)
tag_map = {}
dev_map = {}
base_dev_names = {d.get('name') for d in base['devices'].values()}
for dev_id, dev in other['devices'].items():
    if dev_id in base['devices']:
        new_id = 'd_' + prefix + dev_id[2:] if dev_id.startswith('d_') else prefix + dev_id
        assert new_id not in base['devices'], f'device {new_id} already in the base project'
        dev_map[dev_id] = new_id
        dev = dict(dev, id=new_id)
    if dev.get('name') in base_dev_names:
        dev = dict(dev, name=prefix + dev['name'])
    new_tags = {}
    for tid, tag in dev.get('tags', {}).items():
        nt = prefix + tid.split('_', 1)[1] if '_' in tid else prefix + tid
        tag_map[tid] = nt
        new_tags[nt] = dict(tag, id=nt)
    base['devices'][dev_map.get(dev_id, dev_id)] = dict(dev, tags=new_tags)
known = {t for d in base['devices'].values() for t in d.get('tags', {})}
assert len(known) == sum(len(d.get('tags', {})) for d in base['devices'].values()), 'tag id collision'


def remap_vars(o):
    """replace variableId / variableSrc / deviceId values that point at a renamed tag or device, recursively"""
    if isinstance(o, dict):
        out = {}
        for k, v in o.items():
            if k == 'variableId' and isinstance(v, str):
                out[k] = tag_map.get(v, v)
            elif k in ('variableSrc', 'deviceId') and isinstance(v, str):
                out[k] = dev_map.get(v, v)
            else:
                out[k] = remap_vars(v)
        return out
    if isinstance(o, list):
        return [remap_vars(x) for x in o]
    return o


base_view_ids = {v['id'] for v in base['hmi']['views']}
for view in other['hmi']['views']:
    assert view['id'] not in base_view_ids, f"view {view['id']} already in the base project"
    svg = view['svgcontent']
    # element ids inside the view's layer (keep svg_shift / layer markup untouched), plus the item keys
    ids = set(re.findall(r'\sid="([^"]+)"', svg)) | set(view.get('items', {}))
    ids.discard('svg_shift')
    id_map = {i: prefix + i for i in ids}
    svg = re.sub(r'(\sid=")([^"]+)(")', lambda m: m.group(1) + id_map.get(m.group(2), m.group(2)) + m.group(3), svg)
    svg = re.sub(r'url\(#([^)]+)\)', lambda m: f'url(#{id_map.get(m.group(1), m.group(1))})', svg)
    svg = re.sub(r'(href=")#([^"]+)(")', lambda m: m.group(1) + '#' + id_map.get(m.group(2), m.group(2)) + m.group(3), svg)
    items = {}
    for k, it in view.get('items', {}).items():
        nk = id_map.get(k, k)
        items[nk] = remap_vars(dict(it, id=nk))
    base['hmi']['views'].append(dict(view, svgcontent=svg, items=items))

missing = [it['property']['variableId'] for v in base['hmi']['views'] for it in v.get('items', {}).values()
           if it.get('property', {}).get('variableId') and it['property']['variableId'] not in known]
assert not missing, f'items without tag: {missing[:5]}'
srcs = [it['property']['variableSrc'] for v in base['hmi']['views'] for it in v.get('items', {}).values()
        if (it.get('property') or {}).get('variableSrc') and it['property']['variableSrc'] not in base['devices']]
assert not srcs, f'items with unknown device: {srcs[:5]}'
json.dump(base, sys.stdout, ensure_ascii=False)
