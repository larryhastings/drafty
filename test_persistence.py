#!/usr/bin/env python3

__license__ = """
drafty
Copyright 2023 Eric V. Smith and Larry Hastings

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

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
