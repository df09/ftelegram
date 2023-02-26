from telethon import TelegramClient, errors
from datetime import datetime, timedelta
from tqdm import tqdm
import yaml
import time
import re

api_id = 29350618
api_hash = '1d3d60a614af26ab32058f86f68a1536'
pwd = '/home/df09/init/projects/telegram_filter'

def yml2dict(f):
    with open(f, encoding='utf-8') as f:
        return yaml.safe_load(f)
def dict2yml(data, f, sort=False):
    with open(f, 'w') as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=sort, width=4096)
    return True
def utime(date): return date - timedelta(hours=5)
def ftime(date): return date.strftime('%H:%M:%S/%d.%m')
def get_init_msg(): return f"**{channel}.** {ftime(utime(get_offset()))}"
def get_render(channel, date, text, msg_id):
    header = f'{channel}: {utime(message.date).strftime("%H:%M %A (%d %b)")}'
    return f'[{header}](https://t.me/{channel}/{msg_id})\n\n{text}'

def get_offset():
    last_offset = config[channel]['offset']
    if last_offset:
        return last_offset + timedelta(seconds=1)
    return datetime.now() - timedelta(days=3) + timedelta(hours=5)
def need_send(text):
    if not text: text = ''
    for incl in config[channel]['incls']:
        if not re.search(incl, text, re.IGNORECASE):
            print(f'skip (must incl "{incl}")')
            return False
    for excl in config[channel]['excls']:
        if re.search(excl, text, re.IGNORECASE):
            print(f'skip (need excl "{excl}")')
            return False
    return True
def send(render):
    client.loop.run_until_complete(client.send_message('isushkov_filter', render))
def wait(sec):
    print(f'sleep({sec}s)...')
    for i in tqdm(range(sec)):
        time.sleep(1)

filters = f'{pwd}/f-work.yml'
config = yml2dict(filters)

with TelegramClient(f'{pwd}/isushkov_robot', api_id, api_hash) as client:
    # parse channels
    for channel in config:
        client.loop.run_until_complete(client.send_message('isushkov_filter', get_init_msg()))
        # parse messages
        for message in client.iter_messages(channel, reverse=True,
                offset_date=get_offset(), limit=999):
            print(f'{channel}: {ftime(utime(message.date))}', end=' ')
            if need_send(message.text):
                render = get_render(channel, message.date, message.text, message.id)
                try:
                    send(render)
                    print('>>>> SEND')
                except errors.rpcerrorlist.FloodWaitError as e:
                    wait(int(str(e).split()[3]) + 30)
                    print(f'{channel}: {ftime(utime(message.date))} - send after wait...')
                    send(render)
            # save offset
            config[channel]['offset'] = message.date
            dict2yml(config, filters)
