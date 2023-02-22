#!/usr/bin/env python3
import sys
import ijson

cnt = 0

for record in ijson.items(sys.stdin, "item"):
        cnt = cnt + 1

print(cnt)
