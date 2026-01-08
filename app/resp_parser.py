import io

def read_bytes(buff: io.BytesIO):
    val = buff.readline()
    return val[:-2]

def parse_resp(buff: io.BytesIO):
    op = buff.read(1)
    val = None

    if op in [b'+', b'-', b':']:
        val = read_bytes(buff)
        if op == b':':
            val = int(val)
    elif op == b'$':
        n = read_bytes(buff)
        if int(n) == -1:
            # Empty bytes
            return None
        val = buff.read(int(n))
        buff.read(2)
    elif op == b'*':
        n = read_bytes(buff)
        val = []
        for _ in range(int(n)):
            val.append(parse_resp(buff))
    else:
        pass
    return val
