import asyncio
import string
import yaml
import time
import re
import signal
import sys
from tqdm import tqdm
from telethon import TelegramClient, errors
from datetime import datetime, timedelta

def signal_handler(sig, frame):
    print("Ctrl+C: exit.")
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

# fo
def yml2dict(f):
    with open(f, encoding='utf-8') as f:
        return yaml.safe_load(f)
def dict2yml(data, f, sort=False):
    with open(f, 'w') as f:
        yaml.dump(data, f, allow_unicode=True, sort_keys=sort, width=4096)

# comb text
def define_lang(word):
    ru_chars, en_chars = 0, 0
    for char in word:
        if 'а' <= char <= 'я' or 'А' <= char <= 'Я': ru_chars += 1
        elif 'a' <= char <= 'z' or 'A' <= char <= 'Z': en_chars += 1
    return 'ru' if ru_chars >= en_chars else 'en'
def replace_chars(word, mode):
    ru_en_dict = {'а':'a', 'в':'b', 'с':'c', 'е':'e', 'х':'x', 'р':'p', 'о':'o',
        'у':'y', 'к':'k', 'н':'h', 'м':'m', 'т':'t', 'А':'A', 'В':'B', 'С':'C',
        'Е':'E', 'Н':'H', 'О':'O', 'Р':'P', 'Т':'T', 'К':'K', 'М':'M', 'У':'Y',
        'Х':'X'}
    for k,v in ru_en_dict.items():
        if mode == 'ru2en': word = word.replace(k,v)
        if mode == 'en2ru': word = word.replace(v,k)
    return word
def comb_text(text):
    result = ''
    tokens = re.findall(r'\w+|\s+|[^\w\s]+|.+', text)
    for token in tokens:
        # если токен слово
        if bool(re.match(r'^\w+$', token)):
            lang = define_lang(token)
            if lang == 'ru':
                token = token.replace('ё', 'е')
                token = token.replace('й', 'и')
                token = token.replace('Ё', 'Е')
                token = token.replace('Й', 'И')
                token = replace_chars(token, 'en2ru')
            if lang == 'en':
                token = replace_chars(token, 'ru2en')
        result += token
    return result

# decorators
def cat(text, max_length=100):
    text = text.replace('\n', ' ')
    text = re.sub(r'\s', ' ', text)
    text = re.sub(r' +', ' ', text)
    return text if len(text) < max_length else text[:max_length-3] + "..."
def pretty_msg(channel, m, clean_text):
    header = f'{channel.upper()}: {(m.date - timedelta(hours=4)).strftime("%H:%M %A (%d %b)")}'
    return f'[{header}](https://t.me/{channel}/{m.id})\n\n{clean_text}'
def run_tqdm(msg, sec, chart=' ##'):
    print(msg)
    for i in tqdm(range(sec), ascii=chart):
        time.sleep(1)

# filters and offsets
def get_offset():
    last_offset = offsets[channel]
    if last_offset:
        return last_offset + timedelta(seconds=1)
    return datetime.now() - timedelta(days=3) + timedelta(hours=4)
def apply_chanel_filters(log_pfx, text, channel_ruls):
    # empty
    if not text:
        print(f'{log_pfx} - skip: <empty>')
        return False
    try:
        # incls
        for pattern in channel_ruls['incls']:
            if not re.search(pattern, text, re.IGNORECASE):
                print(f'{log_pfx} - skip: incl "{cat(pattern)}"')
                return False
        # excls
        for pattern in channel_ruls['excls']:
            if re.search(pattern, text, re.IGNORECASE):
                print(f'{log_pfx} - skip: excl "{cat(pattern)}"')
                return False
    except re.error as e:
        print("Некорректное правило фильтрации:", pattern)
        exit()
    return True

# parse/send
def send_to_recv(log_pfx, render, pause=4):
    client.loop.run_until_complete(client.send_message(receiver, render, link_preview=False))
    l = cat(re.sub(r'\(https?://\S+|www\.\S+', '', " ".join(render.splitlines()[2:])))
    print(f'{log_pfx} - SENDED: {l}')
    run_tqdm(msg=f'{log_pfx} - wait {pause}s...', sec=pause, chart=' ..')

def get_keys_to_remove(d):
    # Создаем временный словарь для хранения последних ключей для каждого значения
    temp_dict = {}
    # Проходим по всем парам ключ-значение в исходном словаре
    for key, value in d.items():
        temp_dict[value] = key  # Перезаписываем ключ для каждого значения
    # Создаем список для хранения ключей, которые будут удалены
    keys_to_remove = []
    # Проходим по всем парам ключ-значение в исходном словаре снова
    for key, value in d.items():
        # Если ключ не является последним для данного значения, добавляем его в список на удаление
        if temp_dict[value] != key:
            keys_to_remove.append(key)
    return keys_to_remove

# init vars
receiver = 'grp_filter_work_isushkov'
f_filters = 'filters/work.yml'
f_offsets = 'filters/work-offsets.yml'
filters = yml2dict(f_filters)
offsets = yml2dict(f_offsets)
api_id = 29350618
api_hash = '1d3d60a614af26ab32058f86f68a1536'

with TelegramClient(f'isushkov_robot', api_id, api_hash) as client:
    # channels: parse and send
    for channel in filters:
        if channel.startswith("x_"):
            continue
        for m in client.iter_messages(channel, reverse=True, offset_date=get_offset(), limit=999):
            if not m.text: continue # skip enmpty messages
            clean_text = comb_text(m.text)
            log_pfx = f'parse.{channel.upper()}: {(m.date - timedelta(hours=4)).strftime("%H:%M:%S/%m.%d")}'
            if apply_chanel_filters(log_pfx, clean_text, filters[channel]):
                render = pretty_msg(channel, m, clean_text)
                try:
                    send_to_recv(log_pfx, render)
                except errors.rpcerrorlist.FloodWaitError as e:
                    sec = int(str(e).split()[3]) + 30
                    run_tqdm(f'{log_pfx} - BLOCKED. wait {sec}s...', sec)
                    send_to_recv(log_pfx, render)
            # upd offset
            offsets[channel] = m.date
            dict2yml(offsets, f_offsets)
        print(f'parse.{channel.upper()}: DONE.')

    # receiver: remove empty messages
    m_empty = {}
    for m in client.iter_messages(receiver, reverse=True, limit=999):
        if not m.text: continue # skip admins messages
        # remove all empty lines and extra spaces
        m.text = '\n'.join([line.strip() for line in m.text.splitlines() if line.strip()])
        if len(m.text.splitlines()) == 1:
            if re.search(r'\[[A-Z_]*: \d\d:\d\d \w* \(\d\d \w*\)\]\(https://.*\)', m.text):
                m_empty[m.id] = m.text
    if m_empty:
        m_empty_ids = [str(i) for i in m_empty]
        client.loop.run_until_complete(client.delete_messages(receiver, message_ids=m_empty_ids))
        print(f'RECEIVER: empty messages:')
        for m in m_empty:
            l = cat(f'{str(m)}: "{m_empty[m]}"').replace('\n','\\n')
            print(f'    {l}')
        print(f'RECEIVER: empty messages - DONE.')
    else:
        print(f'RECEIVER: empty messages - None.')

    # receiver: remove dublicated messages
    m_hashes = {}
    for m in client.iter_messages(receiver, limit=999):
        if not m.text: continue # skip admins and empty messages
        # clean text
        clear_text = ''.join(m.text.splitlines()[1:])
        clear_text = re.sub(r'\((tg|http[s]?)://[^\s]*\)', '()', clear_text)
        clear_text = re.sub(r'[^a-zA-Zа-яА-Я0-9]', '', clear_text)
        # done
        m_hashes[m.id] = hash(clear_text)
    keys_to_remove = get_keys_to_remove(m_hashes)
    if keys_to_remove:
        print(f'RECEIVER: dublicated messages: {", ".join(str(key) for key in keys_to_remove)}')
        client.loop.run_until_complete(client.delete_messages(receiver, message_ids=keys_to_remove))
        print(f'RECEIVER: dublicated messages - DONE.')
    else:
        print(f'RECEIVER: dublicated messages - None.')
