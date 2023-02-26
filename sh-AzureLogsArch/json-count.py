#!/usr/bin/env python3
import sys
#import ijson

cnt_open = 0

for line in sys.stdin:
    if line.startswith("  {"):
        cnt_open = cnt_open + 1

print(cnt_open)
