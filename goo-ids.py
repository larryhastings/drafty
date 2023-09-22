import hashlib
import uuid
import bz2
import base64

for _ in range(20):
    guid = str(uuid.uuid1())
    guid = guid.replace('-', '')
    guid = base64.b16decode(guid, casefold=True)
    print(len(guid), guid)

    # guid = str(uuid.uuid1()).encode('ascii')
    # guid2 = hashlib.blake2b(guid).digest()[:12]
    # print(len(guid2), guid2)
    # guid3 = guid.replace(b'-', b'')
    # guid3 = base64.b16decode(guid3, casefold=True)
    # print(len(guid3), guid3)
    print()