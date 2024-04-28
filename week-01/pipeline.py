import pandas as pd
import sys

# some fancy stuff with pandas

fname = sys.argv[0]
day = sys.argv[1]

message = f'job finished successfully through {fname} for day = {day}!'

print(message)