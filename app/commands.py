import time
from .resp_parser import encode_resp

redis_dict = {}

def run_commands(command: list[bytes]) -> bytes:
    match command[0].lower():
        case b'ping':
            return ping_pong()
        case b'echo':
            return echo(command)
        case b'set':
            return set(command)
        case b'get':
            return get(command)
        case b'rpush':
            return rpush(command)
        case b'lrange':
            return lrange(command)
        case b'lpush':
            return lpush(command)
        case b'llen':
            return llen(command)
        case b'lpop':
            return lpop(command)
        case _:
            return encode_resp(b"Not Implemented Yet.", 'simple_error')


def ping_pong() -> bytes:
    return encode_resp(b'PONG', "simple_string")


def echo(command: list[bytes]) -> bytes:
    return encode_resp(command[1], 'bulk_string')


def set(command: list[bytes]) -> bytes:
    expiration_time = None

    if len(command) > 3:
        if command[3].lower() == b'ex':
            expiration_time = time.time() + int(command[4].decode())
        elif command[3].lower() == b'px':
            expiration_time = time.time() + (float(command[4].decode()) / 1000)


    print("Setting: ", command[1], command[2], expiration_time)
    redis_dict[command[1]] = {
        "value": command[2],
        "expires_at": expiration_time
    }
    
    return encode_resp(b'OK', "simple_string")


def get(command: list[bytes]) -> bytes:
    it = redis_dict.get(command[1])
    
    if not it:
        return encode_resp(None)
    
    if it.get("expires_at"):
        if it.get("expires_at") <= time.time():
            del redis_dict[command[1]]
            return encode_resp(None)
    
    return encode_resp(it.get("value"), "bulk_string")


def rpush(command: list[bytes]) -> bytes:
    it = redis_dict.get(command[1])
    if not it:
        redis_dict[command[1]] = {
            "value": []
        }
        it = redis_dict[command[1]]

    for item in command[2:]:
        value_list = it.get("value")
        if not isinstance(value_list, list):
            return encode_resp(b"It's not a list", "simple_error")
        
        value_list.append(item)

    value_list = it.get("value")
    return encode_resp(b"%d" % len(value_list), "integers")


def lrange(command: list[bytes]) -> bytes:
    it = redis_dict.get(command[1])
    if not it:
        return encode_resp([], "array")
    
    value_list = it.get("value")
    if not isinstance(value_list, list):
        return encode_resp(b"WRONGTYPE The value isn't a list", "simple_error")

    l = int(command[2].decode())
    r = int(command[3].decode())

    if l < 0:
        l = len(value_list) + l
    if r < 0:
        r = len(value_list) + r
    
    r = min(r + 1, len(value_list))

    if l > r or l > len(value_list):
        value_list = []
    else:
        value_list = value_list[l:r]
    
    return encode_resp(value_list, "array")


def lpush(command: list[bytes]) -> bytes:
    it = redis_dict.get(command[1])
    if not it:
        redis_dict[command[1]] = {
            "value": []
        }
        it = redis_dict[command[1]]

    for item in command[2:]:
        value_list = it.get("value")
        if not isinstance(value_list, list):
            return encode_resp(b"It's not a list", "simple_error")
        
        value_list.insert(0, item)

    value_list = it.get("value")
    return encode_resp(b"%d" % len(value_list), "integers")


def llen(command: list[bytes]) -> bytes:
    it = redis_dict.get(command[1])
    if not it:
        return encode_resp(b"0", "integers")

    value_list = it.get("value")
    return encode_resp(b"%d" % len(value_list), "integers")


def lpop(command: list[bytes]) -> bytes:
    it = redis_dict.get(command[1])
    if not it:
        return encode_resp(None)
    if len(it.get("value")) == 0:
        return encode_resp(None)
    
    if command[2]:
        to_return = []
        for _ in range(int(command[2].decode())):
            if len(it.get("value")) == 0:
                break
            to_return.append(it.get("value").pop(0))
        return encode_resp(to_return, "array")

    value = it.get("value").pop(0)
    return encode_resp(value, "bulk_string")