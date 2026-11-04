# TCPFTPclient.py

import socket
import os 
import sys
import hashlib

while True:       
    if len(sys.argv)>2:
        Server_ip = sys.argv[1]
        Server_port = int(sys.argv[2])
        break
    else:
        print("[-] Err: please specify server IP & Port to stablish connection")
        exit()

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((Server_ip, Server_port)) 
client_ip, client_port = client.getsockname()

def prog_indicator(progress):
    count = 0
    str=""
    while(count < int(progress/2)):
        str += ("|")
        count += 1
    return str

def PUT(command):
    parsed_cmd = command.split(" ")

    folder = './client_storage'
    file_name = parsed_cmd[1]
    dir_ls = os.listdir(folder)
    if(file_name in dir_ls):
        file_path = os.path.join(folder, file_name)
        file_size = os.path.getsize(file_path)
    else:
        print("[-] ERR: File doesn't Exist") 
        return

    header = f"PUT {file_name} {file_size}".encode()
    client.send(len(header).to_bytes(8, "big"))
    client.send(header)

    chunk = 1024
    bytes_sent = 0
    try: 
        with open(file_path,"rb") as readfile:
            while True:
                data = readfile.read(chunk)
                if not data:
                    break
                client.sendall(data) 
                bytes_sent += len(data)
                prog = int(bytes_sent/file_size*100) if file_size else 100
                print(f"\rProgress: {prog_indicator(prog)} {prog}%", end="")

        res_len = int.from_bytes(client.recv(8), "big")
        res_buf = b""
        while len(res_buf) < res_len:
            res_buf += client.recv(res_len - len(res_buf))
        res = res_buf.decode()
        print(f"\n[+] Response from server: {res}")

        with open(file_path, "rb") as readfile:
            file_content = readfile.read()
            client_hash = hashlib.md5(file_content).hexdigest()

        fin_msg = f"DONE {client_hash}".encode()
        client.send(len(fin_msg).to_bytes(8, "big"))
        client.sendall(fin_msg)

    except Exception as err:
        print(f"[-] ERR: {err}")               

def GET(command):
    parsed_cmd = command.split(" ")
    file_name = parsed_cmd[1]

    folder = './client_storage'
    os.makedirs(folder, exist_ok=True)
    num=0
    name = file_name
    while True:
        if name in os.listdir(folder):
            num += 1
            name_list = file_name.split('.')
            name = f'{name_list[0]}({num}).{name_list[1]}'
        else:
            file_path = os.path.join(folder, name)
            if name != file_name:
                print(f"[-] '{file_name}' already exists, saving as '{name}'")
            break

    bytes_rcvd = 0
    file_size = 0

    try:
        header = f"GET {file_name}".encode()
        client.send(len(header).to_bytes(8, "big"))
        client.sendall(header)

        with open(file_path, 'wb') as write_file:
            while True:
                if(file_size == 0):
                    head_len = int.from_bytes(client.recv(8), "big")
                    head_buf = b""
                    while len(head_buf) < head_len:
                        head_buf += client.recv(head_len - len(head_buf))
                    head = head_buf.decode()
                    parts = head.split(" ")
                    if(parts[0].startswith("ERR")):
                        print(f"Response from server: {head}")
                        break
                    file_size = int(parts[1])
                else:
                    data = client.recv(min(1024, file_size - bytes_rcvd))
                    write_file.write(data)
                    bytes_rcvd += len(data)
                    prog = int(bytes_rcvd/file_size*100)
                    print(f"\rProgress: {prog_indicator(prog)} {prog}%", end="")
                if(bytes_rcvd >= file_size):
                    break
        if not(parts[0].startswith("ERR")):
            res_len = int.from_bytes(client.recv(8), "big")
            res_buf = b""
            while len(res_buf) < res_len:
                res_buf += client.recv(res_len - len(res_buf))
            res = res_buf.decode()
            print(f"\n[+] Response from server: {res}")

            res_len = int.from_bytes(client.recv(8), "big")
            res = b""
            while len(res) < res_len:
                res += client.recv(res_len - len(res))
            msglist = (res.decode()).split(' ')
            msg, server_hash = msglist
            
            with open(file_path, "rb") as readfile:
                file_content = readfile.read()
                client_hash = hashlib.md5(file_content).hexdigest()
            
            print(f"[+] CLIENT HASH: {client_hash}")
            print(f"[+] SERVER HASH: {server_hash}")
                                
            if server_hash == client_hash:
                fin_msg = 'HASH VERIFIED'.encode()
                client.send(len(fin_msg).to_bytes(8, "big"))
                client.sendall(fin_msg)
                print(f"[+] Hash Matched: file received successfully")
            else:
                fin_msg = 'HASH MISMATCH'.encode()
                client.send(len(fin_msg).to_bytes(8, "big"))
                client.sendall(fin_msg)
                print(f"[-] Hash Mismatched: received currupted file")

    except Exception as err:
        print(f"[-] ERR: {err}") 
  
def LIST(command):
    if(command == "LIST"):
        msg = "LIST".encode()
        client.send(len(msg).to_bytes(8, "big"))
        client.send(msg)

        res_len = int.from_bytes(client.recv(8), "big")
        res_buf = b""
        while len(res_buf) < res_len:
            res_buf += client.recv(res_len - len(res_buf))
        data = res_buf.decode()
        files_list = data.split("\n")
        print(f"Total files: {files_list[0].split(" ")[1]}")
        for files in files_list[1:]:
            if(files != ' ' and files != ''):
                print(f"{files} Bytes")
    else:
        print("[-] ERR: Invalid Command!!!")

while True:
    usr_command = input(f"(@{client_port})#>>$ ")
    if usr_command.startswith("QUIT"):
        header = "QUIT".encode()
        client.send(len(header).to_bytes(8, "big"))
        client.sendall(header)
        break
    elif usr_command.startswith("HELP"):
        print("\n1.PUT: PUT <filename>\n2.GET: GET <filename>\n3.LIST: LIST\n4.QUIT: QUIT\n5.CLEAR: CLEAR\n")    
    elif usr_command.startswith("PUT"):
        PUT(usr_command)    
    elif usr_command.startswith("GET"):
        GET(usr_command)
    elif usr_command.startswith("CLEAR"):
            os.system('cls' if os.name == 'nt' else 'clear')
    else: LIST(usr_command)

client.close() 