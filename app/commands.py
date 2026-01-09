from .resp_parser import encode_resp

def run_commands(command: list[bytes]) -> bytes:
    match command[0].lower():
        case b'ping':
            return encode_resp(b'PONG', "simple_string")
        case b'echo':
            return encode_resp(command[1], 'bulk_string')
        case _:
            return encode_resp(b"Not Implemented Yet.", 'simple_error')

