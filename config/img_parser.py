#!/usr/bin/env python3 
import sys

img = {}

block = 1
block_size = 1
band = 1
pixel = 1

for line in sys.stdin:
    print(block, band, pixel, line)

    # if pixel == 1024:
    #     pixel = 1
    #     block += 1
    band += 1
    if band == 181:
        band = 1
        if pixel == 1024:
            pixel = 1
            block += 1
        else:
            pixel += 1

    