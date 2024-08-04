#!/usr/bin/env python3

import sys
import traceback
import json
import glob
import sqlite3
import os
import re

with open("config.json", "r") as cf:
    config = json.load(cf)

input = config.get('input', {})
if not input.get('file_pattern'):
    raise RuntimeError("file_pattern is not defined.")
input['encoding'] = input.get('encoding', 'UTF-8')

output = config.get('output', [])
for entry in output:
    if not entry or not isinstance(entry, dict):
        raise RuntimeError("output has an invalid entry")
    if not entry.get('file'):
        raise RuntimeError("file must be defined in output entry.")
    if entry.get('text_pattern'):
        try:
            entry['text_pattern'] = re.compile(entry['text_pattern'].encode(input['encoding']))
        except re.error as e:
            raise RuntimeError(f"text_pattern is invalid: {entry['text_pattern']}")

con = sqlite3.connect('logwatch.dat')
try:
    con.execute('CREATE TABLE IF NOT EXISTS file_status (file_name TEXT PRIMARY KEY, pos INTEGER)')

    map = dict()
    for row in con.execute('SELECT file_name, pos FROM file_status ORDER BY file_name'):
        map[row[0]] = { 'pos': row[1], 'state': 'D' }

    for file_name in glob.glob(input['file_pattern'], recursive=True):
        offset = 0
        size = os.path.getsize(file_name)
        row = map.get(file_name)

        if row:
            if row['pos'] == size:
                del map[file_name]
                continue
            else:
                offset = row['pos']
                row['state'] = 'U'
                row['pos'] = size
        else:
            map[file_name] = { 'pos': size, 'state': 'I' }
        
        with open(file_name, "rb") as f:
            if offset > 0:
                f.seek(offset)
            
            buf = []
            for line in f:
                if line.endswith(b'\n'):
                    buf.append(line)
                else:
                    row['pos'] = size - len(line)
                    break
            
            if len(output) > 0:
                for entry in output:
                    with open(entry['file'], "a+b") as of:
                        text_pattern = entry.get('text_pattern')
                        if text_pattern:
                            for line in buf:
                                if text_pattern.search(line):
                                    of.write(line)
                        else:
                            for line in buf:
                                of.write(line)
            else:
                for line in buf:
                    sys.stdout.buffer.write(line)

    for file_name in map:
        row = map[file_name]
        if row['state'] == 'I':
            con.execute('INSERT INTO file_status (file_name, pos) VALUES (?, ?)', [file_name, row['pos']])
            # print(f'add {file_name}')
        elif row['state'] == 'D':
            con.execute('DELETE FROM file_status WHERE file_name = ?', [file_name])
            # print(f'delete {file_name}')
        else:
            con.execute('UPDATE file_status SET pos = ? WHERE file_name = ?', [row['pos'], file_name])
            # print(f'update {file_name}')
    
    con.commit()
except Exception as e:
    con.rollback()
    print(traceback.format_exc(), file=sys.stderr)
finally:
    con.close()
