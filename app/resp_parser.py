import io
from typing import Literal

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

def encode_resp(
        data: bytes, 
        encoding_type: Literal[
            "simple_string", 
            "simple_error", 
            "integers", 
            "bulk_string", 
            "array"] = "simple_string"
            ):
    match encoding_type:
        case "simple_string":
            return b'+' + data + b'\r\n'
        case "simple_error":
            return b'-' + data + b'\r\n'
        case "integers":
            return b':' + data + b'\r\n'
        case "bulk_string":
            return b'$%d\r\n%s\r\n' %  (len(data), data)
        # TODO array
        case _:
            return b'-Encoding not implemented.\r\n'
