from telethon import TelegramClient, errors
from datetime import datetime, timedelta
from tqdm import tqdm
import yaml
import time
import re
import signal
import sys
def signal_handler(sig, frame):
    print("Ctrl+C: exit.")
    sys.exit(0)

# formaters
def yml2dict(f):
    with open(f, encoding='utf-8') as f:
        return yaml.safe_load(f)
def dict2yml(data, f, sort=False):
    with open(f, 'w') as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=sort, width=4096)
    return True
def utime(date): return date - timedelta(hours=5)
def ftime(date): return date.strftime('%H:%M:%S/%d.%m')
# flatten
def flatte_filter_ruls(ruls):
    flatten = []
    for r in ruls:
        if isinstance(r, str):
            flatten.append(r)
            continue
        if isinstance(r, list):
            for i in r:
                flatten.append(i)
    return flatten

# decorators
def send_start_msg():
    client.loop.run_until_complete(client.send_message(receiver, get_start_msg()))
def get_start_msg(): return f"**{channel}** >{ftime(utime(get_offset()))}"
def get_render(channel, date, text, msg_id):
    header = f'{channel.upper()}: {utime(message.date).strftime("%H:%M %A (%d %b)")}'
    return f'[{header}](https://t.me/{channel}/{msg_id})\n\n{text}'
def filter_exeption(kind, rule=False, max_length=60):
    if kind == 'empty':
        print('skip (__empty__)')
        return False
    if not rule:
        exit('filter_exeption: error')
    if len(rule) > max_length:
        rule = rule[:max_length-3] + "..."
    print(f'skip ({kind} "{rule}")')
    return False

# main logic
def get_offset():
    last_offset = offsets[channel]
    if last_offset:
        return last_offset + timedelta(seconds=1)
    return datetime.now() - timedelta(days=3) + timedelta(hours=5)
def need_send(text, rules):
    # empty
    if not text:
        return filter_exeption('empty')
    # incls
    for incl in flatte_filter_ruls(rules['incls']):
        if not re.search(incl, text, re.IGNORECASE):
            return filter_exeption('incl', incl)
    # excls
    for excl in flatte_filter_ruls(rules['excls']):
        if re.search(excl, text, re.IGNORECASE):
            return filter_exeption('excl', excl)
    # excls-multiline
    for excl_multi in flatte_filter_ruls(rules['excls-multi']):
        if re.search(excl_multi, text, re.IGNORECASE, re.MULTILINE)
            return filter_exeption('excls-multi', excl_multi)
    return True
def send(render):
    client.loop.run_until_complete(client.send_message(
        receiver, render, link_preview=False))
def wait(sec):
    print(f'sleep({sec}s)...')
    for i in tqdm(range(sec)):
        time.sleep(1)

# init vars
signal.signal(signal.SIGINT, signal_handler)
receiver = 'grp_filter_work_isushkov'
f_filters = 'filters/work.yml'
f_offsets = 'filters/work-offsets.yml'
filters = yml2dict(f_filters)
offsets = yml2dict(f_offsets)
# start cli
api_id = 29350618
api_hash = '1d3d60a614af26ab32058f86f68a1536'

with TelegramClient(f'isushkov_robot', api_id, api_hash) as client:
    # channel
    for channel in filters:
        if channel.startswith("x_"):
            continue
        # parse and send messages
        for message in client.iter_messages(channel, reverse=True,
                offset_date=get_offset(), limit=999):
            print(f'{channel}: {ftime(utime(message.date))}', end=' ')
            if need_send(message.text, filters[channel]):
                render = get_render(channel, message.date, message.text, message.id)
                try:
                    # send
                    send(render)
                    print('>>>> SEND')
                except errors.rpcerrorlist.FloodWaitError as e:
                    wait(int(str(e).split()[3]) + 30)
                    print(f'{channel}: {ftime(utime(message.date))} - send after wait...')
                    send(render)
            # save offset
            offsets[channel] = message.date
            dict2yml(offsets, f_offsets)
        # post-manage: remove dublicates
        m_hashs = {}
        for m in client.iter_messages(receiver, reverse=True, limit=999):
            # filter
            if not m.text:
                continue
            lines = m.text.splitlines()
            if len(lines) < 3:
                continue
            # remove dublicates
            hs = hash(''.join(lines[2:]))
            dublicates = [mid for mid,mhash in m_hashs.items() if hs == mhash]
            if dublicates:
                client.loop.run_until_complete(client.delete_messages(
                    receiver, message_ids=dublicates))
                print(f'{channel}: remove dublicates - {dublicates}')
            # save hash
            m_hashs[m.id] = hs
