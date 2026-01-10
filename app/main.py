import threading
import socket  # noqa: F401
import io
from .resp_parser import parse_resp, encode_resp
from .commands import run_commands


def handle_input(client_conn: socket.socket):
    while True:
        client_msg_raw = client_conn.recv(1024)
        if not client_msg_raw:
            break
        print("Client said:", client_msg_raw)

        buff = io.BytesIO(client_msg_raw)
        commands = parse_resp(buff)

        print("Parsed commands:", commands)

        if isinstance(commands, list):
            response = run_commands(commands)
        else:
            response = encode_resp(b"Input was not a list...", "simple_error")
        
        client_conn.send(response)

    
    print("Closing Connection!")
    client_conn.close()


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        client_conn, _ = server_socket.accept() # wait for client
        threading.Thread(target=handle_input, args=(client_conn,)).start()


if __name__ == "__main__":
    main()
