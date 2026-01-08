import threading
import socket  # noqa: F401
from resp_parser import parse_resp


def handle_input(client_conn: socket.socket):
    while True:
        client_msg = client_conn.recv(1024)
        if not client_msg:
            break
        print("Client said:", client_msg)

        if client_msg == b'$4\r\nPING\r\n':
            client_conn.send(b"+PONG\r\n")
        elif client_msg.startswith(b'*2\r\n$4\r\nECHO\r\n'):
            to_send = client_msg.replace(b'*2\r\n$4\r\nECHO\r\n', b'')
            client_conn.send(to_send)
    
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
