#!/usr/bin/env python3


import contextlib

@contextlib.contextmanager
def overwrite_and_keep_open(f):
    f.seek(0)
    f.truncate()
    yield
    f.flush()

filenames_and_modes = (
    ("binary_file.bin", "wb"),
    ("text_file.txt", "wt"),
    )

handles = [open(*t) for t in filenames_and_modes]
# {binary, text} file handles
bfh, tfh = handles

bfh.write(b"We're charging our battery\nAnd now we're full of energy\nWe are the robots.\n")
tfh.write( "We're functioning automatic\nAnd we are dancing mechanic\nWe are the robots.\n")

[f.close() for f in handles]


handles = [open(*t) for t in filenames_and_modes]
bfh, tfh = handles

with overwrite_and_keep_open(bfh):
    bfh.write(b'Wir fahren, fahren, fahren auf der Autobahn')

with overwrite_and_keep_open(tfh):
    tfh.write('Radioactivity\nIs in the air for you and me')

print('oh no! disaster strikes!')
value = 1 / 0
