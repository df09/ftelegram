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

# обработка текста
def count_characters(word):
    ru_count = 0
    en_count = 0
    for char in word:
        if 'а' <= char <= 'я' or 'А' <= char <= 'Я': ru_count += 1
        elif 'a' <= char <= 'z' or 'A' <= char <= 'Z': en_count += 1
    return ru_count, en_count
def get_lang(word):
    ru_char, en_char = count_characters(word)
    return 'ru' if ru_char >= en_char else 'en'
def replace_chars(word, mode):
    ru_en_dict = {'а':'a', 'в':'b', 'с':'c', 'е':'e', 'х':'x', 'р':'p', 'о':'o',
        'у':'y', 'к':'k', 'н':'h', 'м':'m', 'т':'t', 'А':'A', 'В':'B', 'С':'C',
        'Е':'E', 'Н':'H', 'О':'O', 'Р':'P', 'Т':'T', 'К':'K', 'М':'M', 'У':'Y',
        'Х':'X'}
    for k,v in ru_en_dict.items():
        if mode == 'ru2en': word = word.replace(k,v)
        if mode == 'en2ru': word = word.replace(v,k)
    return word
def fix_en_ru_chars(trash_text):
    clear_text = ''
    tokens = re.findall(r'\w+|\d+|\s+|[^\w\d\s]+|.+', trash_text)
    for token in tokens:
        # если токен слово
        if bool(re.match(r'^\w+$', token)):
            if get_lang(token) == 'ru':
                token = token.replace('ё', 'е')
                token = token.replace('й', 'и')
                token = token.replace('Ё', 'Е')
                token = token.replace('Й', 'И')
                token = replace_chars(token, 'en2ru')
            if get_lang(token) == 'en':
                token = replace_chars(token, 'ru2en')
            clear_text += token
        else:
            clear_text += token
    return clear_text
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
def cat(text, max_length=100):
    text = text.replace('\n', ' ')
    text = re.sub(r'\s', ' ', text)
    text = re.sub(r' +', ' ', text)
    return text if len(text) < max_length else text[:max_length-3] + "..."
def pretty_msg(channel, m, clean_text):
    header = f'{channel.upper()}: {(m.date - timedelta(hours=5)).strftime("%H:%M %A (%d %b)")}'
    return f'[{header}](https://t.me/{channel}/{m.id})\n\n{clean_text}'
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
    try:
        # incls
        for pattern in flatte_filter_ruls(channel_ruls['incls']):
            if not re.search(pattern, text, re.IGNORECASE):
                print(f'{log_pfx} - skip: incl "{cat(pattern)}"')
                return False
        # excls
        for pattern in flatte_filter_ruls(channel_ruls['excls']):
            if re.search(pattern, text, re.IGNORECASE):
                print(f'{log_pfx} - skip: excl "{cat(pattern)}"')
                return False
        # excls-multiline
        for pattern in flatte_filter_ruls(channel_ruls['excls-multi']):
            if re.search(pattern, text, re.IGNORECASE | re.MULTILINE):
                print(f'{log_pfx} - skip: excl_multi "{cat(pattern)}"')
                return False
    except re.error as e:
        print("Некорректное правило фильтрации:", pattern)
        exit()
    return True
def run_tqdm(msg, sec, chart=' ##'):
    print(msg)
    for i in tqdm(range(sec), ascii=chart):
        time.sleep(1)

def send_to_recv(log_pfx, render, pause=4):
    client.loop.run_until_complete(client.send_message(receiver, render, link_preview=False))
    l = cat(re.sub(r'\(https?://\S+|www\.\S+', '', " ".join(render.splitlines()[2:])))
    print(f'{log_pfx} - SENDED: {l}')
    run_tqdm(msg=f'{log_pfx} - SENDED: avoid blocking. wait {pause}s...', sec=pause, chart=' ..')
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
            if not m.text: continue # skip enmpty messages
            clean_text = fix_en_ru_chars(m.text)
            log_pfx = f'parse.{channel.upper()}: {(m.date - timedelta(hours=5)).strftime("%H:%M:%S/%d.%m")}'
            if apply_chanel_filters(log_pfx, clean_text, filters[channel]):
                render = pretty_msg(channel, m, clean_text)
                try:
                    send_to_recv(log_pfx, render)
                except errors.rpcerrorlist.FloodWaitError as e:
                    sec = int(str(e).split()[3]) + 30
                    run_tqdm(f'{log_pfx} - wait {sec}s...', sec)
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
            l = cat(f'{str(m)}: "{m_empty[m]}"').replace('\n','\\n')
            print(f'    {l}')
    print(f'RECEIVER: empty messages - DONE.')
    # receiver: remove dublicated
    m_hashs = {}
    c = 0
    for m in client.iter_messages(receiver, limit=999):
        if not m.text: continue # skip admins messages
        # prepare msg
        clear_text = ''.join(m.text.splitlines()[1:])
        clear_text = re.sub(r'\((tg|http[s]?)://[^\s]*\)', '()', clear_text)
        clear_text = re.sub(r'[^a-zA-Zа-яА-Я]', '', clear_text)
        m_hash = hash(clear_text)
        m_dublicated_ids = [mid for mid,mhash in m_hashs.items() if mhash == m_hash]
        if m_dublicated_ids:
            c += 1
            if c == 1:
                print(f'RECEIVER: dublicated messages was removed:')
            client.loop.run_until_complete(client.delete_messages(receiver, message_ids=[str(m.id)]))
            l = cat(re.sub(r'\(https?://\S+|www\.\S+', '', f'{str(m.id)}: "{m.text}"').replace('\n',' '))
            print(f'    {l}')
        else:
            m_hashs[m.id] = m_hash
    print(f'RECEIVER: dublicated messages - DONE.')
    # receiver: save report
    msgs = []
    for m in client.iter_messages(receiver, reverse=True, offset_date=get_offset(), limit=999):
        msgs.append(process_message(m.text))
    file_path = 'report.wiki'
    with open(file_path,'w') as f:
        f.write('\n'.join(msgs))
    print(f'REPORT: done.')

# # send report
# asyncio.run(send_report())
