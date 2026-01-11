import time
from threading import Condition, Lock
from .resp_parser import encode_resp

redis_dict = {}
dict_lock = Lock()

def run_commands(command: list[bytes]) -> bytes:
    match command[0].lower():
        case b'ping':
            return ping_pong()
        case b'echo':
            return echo(command)
        case b'set':
            with dict_lock:
                return set(command)
        case b'get':
            with dict_lock:
                return get(command)
        case b'rpush':
            with dict_lock:
                return rpush(command)
        case b'lrange':
            with dict_lock:
                return lrange(command)
        case b'lpush':
            with dict_lock:
                return lpush(command)
        case b'llen':
            with dict_lock:
                return llen(command)
        case b'lpop':
            with dict_lock:
                return lpop(command)
        case b'blpop':
            return blpop(command)
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
        if it.get("conditions"):
            condition = it.get("conditions")[0]
            condition.notify()
            it.get("conditions").pop(0)
            

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

        if it.get("conditions"):
            condition = it.get("conditions")[0]
            condition.notify()
            it.get("conditions").pop(0)
            


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


def blpop(command: list[bytes]) -> bytes:

    with dict_lock:
        it = redis_dict.get(command[1])
        if it and len(it.get("value")) > 0:
            value = it.get("value").pop(0)
            return encode_resp(value, "bulk_string")
    
        if not it or len(it.get("value")) == 0:
            condition = Condition(dict_lock)
            if not it:
                redis_dict[command[1]] = {}
                it = redis_dict[command[1]]
            
            if not it.get("conditions"):
                it["conditions"] = []
            
            if not it.get("value"):
                it["value"] = []
            
            if not isinstance(it["value"], list):
                return encode_resp(b"It's not a list", "simple_error")
            
            it["conditions"].append(condition)

            timeout = 0.0
            if len(command) > 2:
                timeout = float(command[2].decode())

            while not len(it.get("value")) > 0:
                print("Waiting...")
                if timeout != 0:
                    if not condition.wait(timeout=timeout):
                        # Timeout occurred
                        it.get("conditions").remove(condition)
                        return encode_resp(None)
                else:
                    condition.wait()

            val = it.get("value").pop(0)
            return encode_resp(val, "bulk_string")
        return encode_resp(b"Unexpected Error Occured", "simple_error")
            