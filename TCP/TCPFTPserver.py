import sys
import socket 
import threading
import os
import hashlib

HOST = "0.0.0.0"
PORT = ''

folder = './server_storage'
os.makedirs(folder, exist_ok=True)

def new_client(client, addr):
    try: 
        while True:
            cmd_len = int.from_bytes(client.recv(8), "big")
            cmd_buf = b""
            while len(cmd_buf) < cmd_len:
                cmd_buf += client.recv(cmd_len - len(cmd_buf))
            file_data = cmd_buf.decode()
            parsed_data = file_data.split(" ")

            if parsed_data[0] == "PUT":
                print(f"\n[{addr[0]}:{addr[1]}] Client want to send a file")
                print(f"[{addr[0]}:{addr[1]}] File Name: {parsed_data[1]} File Size: {parsed_data[2]}")
                print(f"[{addr[0]}:{addr[1]}] File receiving initiated...")
                num=0
                file_name = parsed_data[1]
                name = file_name
                while True:
                    if name in os.listdir(folder):
                        num += 1
                        name_list = file_name.split('.')
                        name = f'{name_list[0]}({num}).{name_list[1]}'
                    else:
                        file_path = os.path.join(folder, name)
                        if name != file_name:
                            print(f"[{addr[0]}:{addr[1]}] [-] '{file_name}' already exists, saving as '{name}'")
                        break
                try:
                    rcvd_data = 0
                    file_size = int(parsed_data[2])
                    with open(file_path, 'wb') as write_file:
                        while rcvd_data < file_size:
                            data = client.recv(min(1024, file_size - rcvd_data))
                            write_file.write(data)
                            rcvd_data += len(data)

                    print(f"[{addr[0]}:{addr[1]}] [+] Data Received: {rcvd_data} Bytes")
                    msg = f"OK {parsed_data[2]} Bytes Received".encode()
                    client.send(len(msg).to_bytes(8, "big"))
                    client.sendall(msg)

                    res_len = int.from_bytes(client.recv(8), "big")
                    res = b""
                    while len(res) < res_len:
                        res += client.recv(res_len - len(res))
                    msglist = (res.decode()).split(' ')
                    msg, client_hash = msglist

                    with open(file_path, "rb") as readfile:
                        file_content = readfile.read()
                        server_hash = hashlib.md5(file_content).hexdigest()

                    print(f"[{addr[0]}:{addr[1]}] [+] CLIENT HASH: {client_hash}")
                    print(f"[{addr[0]}:{addr[1]}] [+] SERVER HASH: {server_hash}")
                    
                    if server_hash == client_hash:
                        fin_msg = 'HASH VERIFIED'.encode()
                        client.send(len(fin_msg).to_bytes(8, "big"))
                        client.sendall(fin_msg)
                        print(f"[{addr[0]}:{addr[1]}] [+] Hash Matched: file received successfully")
                    else:
                        fin_msg = 'HASH MISMATCH'.encode()
                        client.send(len(fin_msg).to_bytes(8, "big"))
                        client.sendall(fin_msg)
                        print(f"[{addr[0]}:{addr[1]}] [-] Hash Mismatched: received currupted file")

                except Exception as err:
                    print(f"[{addr[0]}:{addr[1]}] [-] Err: {err}")
                    error = str(err).encode()
                    client.send(len(error).to_bytes(8, "big"))
                    client.sendall(error)


            elif parsed_data[0] == "GET":
                print(f"[{addr[0]}:{addr[1]}] Client Requested for file: {parsed_data[1]}")
                print(f"[{addr[0]}:{addr[1]}] File search initiated...")

                try:
                    sent_data = 0
                    file_name = parsed_data[1]
                    dir_ls = os.listdir(folder)

                    if(file_name in dir_ls):
                        file_path = os.path.join(folder, file_name)
                        file_size = os.path.getsize(file_path)
                        remain = file_size
                    else:
                        print(f"[{addr[0]}:{addr[1]}] [-] ERR: Requested file doesn't Exist") 
                        err = f"ERR: File Not Found"
                        error = err.encode()
                        client.send(len(error).to_bytes(8, "big"))
                        client.sendall(error)
                        continue

                    header = f"OK {file_size}".encode()
                    client.send(len(header).to_bytes(8, "big"))
                    client.send(header)

                    with open(file_path, 'rb') as read_file:
                        while sent_data < file_size:
                            file_data = read_file.read(min(1024, remain))
                            client.sendall(file_data)
                            sent_data += len(file_data)
                            remain -= len(file_data)

                    print(f"[{addr[0]}:{addr[1]}] [+] File Sent Successfully")
                    msg = f"OK {file_size} Bytes".encode()
                    client.send(len(msg).to_bytes(8, "big"))
                    client.sendall(msg)
                    
                    with open(file_path, "rb") as readfile:
                        file_content = readfile.read()
                        server_hash = hashlib.md5(file_content).hexdigest()
                    
                    fin_msg = f"DONE {server_hash}".encode()
                    client.send(len(fin_msg).to_bytes(8, "big"))
                    client.sendall(fin_msg)

                except Exception as err:
                    print(f"[{addr[0]}:{addr[1]}] [-] Err: {err}")
                    error = str(err).encode()
                    client.send(len(error).to_bytes(8, "big"))
                    client.sendall(error)

            elif parsed_data[0] == "LIST":
                print(f"\n[{addr[0]}:{addr[1]}] Client asked for file list")
                try:
                    list = os.listdir(folder)
                    file_list = 'OK ' + str(len(list)) + '\n'

                    for f in list:
                        file_path = os.path.join(folder, f)
                        file_size = os.path.getsize(file_path)
                        file_list += str(f' {f}  {file_size}\n')

                    list_data = file_list.encode()
                    client.send(len(list_data).to_bytes(8, "big"))
                    client.sendall(list_data)
                    print(f"[{addr[0]}:{addr[1]}] [+] File list sent successfully")
                except Exception as err:
                    print(f"[{addr[0]}:{addr[1]}] [-] Err: {err}")
                    error = str(err).encode()
                    client.send(len(error).to_bytes(8, "big"))
                    client.sendall(error)

            elif parsed_data[0] == "QUIT":
                print(f"[{addr[0]}:{addr[1]}] [-] Client Disconnected")
                break
    except Exception as err:
        print(f"[{addr[0]}:{addr[1]}] [-] Err: {err}")
    finally:
        client.close()
        print(f"[{addr[0]}:{addr[1]}] [-] ACTIVE CLIENT {threading.active_count()-2}")


while True:       
    if len(sys.argv)>1:
        PORT = int(sys.argv[1])
        break
    else:
        print("[-] Err: please specify port to start server")
        exit()

try:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()
    print(f"TCP server is listening on {HOST}:{PORT}")
except Exception as err:
    print(f"[-] ERR: {err}")
    exit()

while True:
    client, addr = server.accept()
    client_thread = threading.Thread(
        target = new_client,
        args=(client, addr)
    )
    client_thread.start()
    print(f"[+] New client connected at port {addr}")
    print(f"[+] ACTIVE CLIENT {threading.active_count()-1}")