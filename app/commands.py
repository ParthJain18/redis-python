import time
from .resp_parser import encode_resp

redis_dict = {}

def run_commands(command: list[bytes]) -> bytes:
    match command[0].lower():
        case b'ping':
            return encode_resp(b'PONG', "simple_string")
        case b'echo':
            return encode_resp(command[1], 'bulk_string')
        
        case b'set':
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
        
        case b'get':
            it = redis_dict.get(command[1])
            
            if not it:
                return encode_resp(None)
            
            if it.get("expires_at"):
                if it.get("expires_at") <= time.time():
                    del redis_dict[command[1]]
                    return encode_resp(None)
            
            return encode_resp(it.get("value"), "bulk_string")
            
        case _:
            return encode_resp(b"Not Implemented Yet.", 'simple_error')

