from telethon import TelegramClient, errors
import asyncio
# from telethon.tl import functions
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
def flatted(f, max_length=100):
    return f if len(f) < max_length else f[:max_length-3] + "..."
def pretty_msg(channel, m):
    header = f'{channel.upper()}: {(m.date - timedelta(hours=5)).strftime("%H:%M %A (%d %b)")}'
    return f'[{header}](https://t.me/{channel}/{m.id})\n\n{m.text}'
# logic
def get_offset():
    last_offset = offsets[channel]
    if last_offset:
        return last_offset + timedelta(seconds=1)
    return datetime.now() - timedelta(days=3) + timedelta(hours=5)
def apply_chanel_filters(log_pfx, text, channel_ruls):
    # empty
    if not text:
        print(f'{log_pfx} - skip: <empty>')
        return False
    # incls
    for incl in flatte_filter_ruls(channel_ruls['incls']):
        if not re.search(incl, text, re.IGNORECASE):
            print(f'{log_pfx} - skip: incl "{flatted(incl)}"')
            return False
    # excls
    for excl in flatte_filter_ruls(channel_ruls['excls']):
        if re.search(excl, text, re.IGNORECASE):
            print(f'{log_pfx} - skip: excl "{flatted(excl)}"')
            return False
    # excls-multiline
    for excl_multi in flatte_filter_ruls(channel_ruls['excls-multi']):
        if re.search(excl_multi, text, re.IGNORECASE | re.MULTILINE):
            print(f'{log_pfx} - skip: excl_multi "{flatted(excl_multi)}"')
            return False
    return True
def send_to_recv(log_pfx, render):
    client.loop.run_until_complete(client.send_message(
        receiver, render, link_preview=False))
    l = flatted(re.sub(r'\(https?://\S+|www\.\S+', '', " ".join(render.splitlines()[2:])))
    print(f'{log_pfx} - SENDED: {l}')
def process_message(m):
    # обработка длинных строк
    processed_lines = []
    for line in [' '.join(line.split()) for line in m.split('\n')]:
        if len(line) <= 80:
            processed_lines.append(line)
        else:
            words = line.split()
            current_line = ""
            for word in words:
                if len(current_line + word) + 1 <= 80:
                    if current_line:
                        current_line += " " + word
                    else:
                        current_line = word
                else:
                    processed_lines.append(current_line)
                    current_line = word
            if current_line:
                processed_lines.append(current_line)
    # Удалить пустые строки, добавить пустую строку в конце
    non_empty_lines = [line for line in processed_lines if line.strip()]
    return '\n'.join(non_empty_lines) + '\n'
# async def send_report():
#     client = TelegramClient('isushkov_robot', api_id, api_hash)
#     await client.start()
#     file_path = "report.txt"
#     input_file = await client.upload_file(file_path)
#     await client.send_file(receiver, input_file, caption='report', link_preview=False)
#     await client.disconnect()
#     print(f'RECEIVER: send report - DONE.')

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
    # channels: parse
    for channel in filters:
        if channel.startswith("x_"):
            continue
        for m in client.iter_messages(channel, reverse=True, offset_date=get_offset(), limit=999):
            log_pfx = f'parse.{channel.upper()}: {(m.date - timedelta(hours=5)).strftime("%H:%M:%S/%d.%m")}'
            if apply_chanel_filters(log_pfx, m.text, filters[channel]):
                render = pretty_msg(channel, m)
                try:
                    send_to_recv(log_pfx, render)
                except errors.rpcerrorlist.FloodWaitError as e:
                    sec = int(str(e).split()[3]) + 30
                    print(f'{log_pfx} - wait {sec}s...')
                    for i in tqdm(range(sec)):
                        time.sleep(1)
                    send_to_recv(log_pfx, render)
            # upd offset
            offsets[channel] = m.date
            dict2yml(offsets, f_offsets)
        print(f'parse.{channel.upper()}: done.')

    # receiver: remove empty
    m_empty = {}
    for m in client.iter_messages(receiver, reverse=True, limit=999):
        if not m.text: continue # skip admins messages
        # remove all empty lines and extra spaces
        m.text = '\n'.join([line.strip() for line in m.text.splitlines() if line.strip()])
        if len(m.text.splitlines()) == 1:
            if re.search('\[[A-Z_]*: \d\d:\d\d \w* \(\d\d \w*\)\]\(https://.*\)', m.text):
                m_empty[m.id] = m.text
    if m_empty:
        m_empty_ids = [str(i) for i in m_empty]
        client.loop.run_until_complete(client.delete_messages(receiver, message_ids=m_empty_ids))
        print(f'RECEIVER: empty messages was removed:')
        for m in m_empty:
            l = flatted(f'{str(m)}: "{m_empty[m]}"').replace('\n','\\n')
            print(f'    {l}')
    print(f'RECEIVER: empty messages - DONE.')

    # receiver: remove dublicated
    m_hashs = {}
    c = 0
    for m in client.iter_messages(receiver, limit=999):
        if not m.text: continue # skip admins messages
        m_hash = hash(''.join(m.text.splitlines()[1:]))
        m_dublicated_ids = [mid for mid,mhash in m_hashs.items() if mhash == m_hash]
        if m_dublicated_ids:
            c += 1
            if c == 1:
                print(f'RECEIVER: dublicated messages was removed:')
            client.loop.run_until_complete(client.delete_messages(receiver, message_ids=[str(m.id)]))
            l = flatted(re.sub(r'\(https?://\S+|www\.\S+', '', f'{str(m.id)}: "{m.text}"').replace('\n',' '))
            print(f'    {l}')
        else:
            m_hashs[m.id] = m_hash
    print(f'RECEIVER: dublicated messages - DONE.')

    # save report
    msgs = []
    for m in client.iter_messages(receiver, reverse=True, offset_date=get_offset(), limit=999):
        msgs.append(process_message(m.text))
    file_path = 'report.wiki'
    with open(file_path,'w') as f:
        f.write('\n'.join(msgs))
    print(f'REPORT: done.')

# # send report
# asyncio.run(send_report())
