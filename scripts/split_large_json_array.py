#!/usr/bin/env python3
"""
Stream-split a large JSON array file into NDJSON (one JSON object per line)
Safe for very large files because it doesn't load the whole file into memory.
Usage: python3 scripts/split_large_json_array.py <input.json> <output.ndjson>
"""
import sys

def split_array_to_ndjson(in_path, out_path):
    with open(in_path, 'r', encoding='utf-8') as fin, open(out_path, 'w', encoding='utf-8') as fout:
        # find first non-whitespace character
        while True:
            ch = fin.read(1)
            if ch == '':
                raise SystemExit('Empty input file')
            if ch.isspace():
                continue
            if ch == '[':
                break
            # if file starts with '{', assume single JSON object
            if ch == '{':
                # read rest and write single-line json
                rest = fin.read()
                obj = ch + rest
                fout.write(obj.strip() + "\n")
                return
            raise SystemExit('Unexpected JSON start char: {!r}'.format(ch))

        buf = []
        depth = 0
        in_str = False
        esc = False
        started = False

        while True:
            chunk = fin.read(8192)
            if not chunk:
                break
            for c in chunk:
                if not started:
                    if c.isspace():
                        continue
                    if c == '{':
                        started = True
                        depth = 1
                        buf.append(c)
                        continue
                    if c == ']':
                        # empty array end
                        return
                    # skip commas or other separators
                    continue
                else:
                    buf.append(c)
                    if esc:
                        esc = False
                        continue
                    if c == '\\':
                        esc = True
                        continue
                    if c == '"':
                        in_str = not in_str
                        continue
                    if not in_str:
                        if c == '{':
                            depth += 1
                        elif c == '}':
                            depth -= 1
                            if depth == 0:
                                s = ''.join(buf).strip()
                                # remove trailing comma if present
                                if s.endswith(','):
                                    s = s[:-1].rstrip()
                                if s:
                                    fout.write(s + '\n')
                                buf = []
                                started = False
        # If we finish but buffer still contains whitespace/newlines, ignore
        # If we have a partial object it's likely malformed

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: {} <input.json> <output.ndjson>'.format(sys.argv[0]))
        sys.exit(2)
    in_path = sys.argv[1]
    out_path = sys.argv[2]
    split_array_to_ndjson(in_path, out_path)
    print('Wrote', out_path)
