# UDPFTPclient.py

import socket
import os
import sys
import struct
import hashlib
import time

chunk_size = 1000

while True:
    if len(sys.argv) > 2:
        Server_ip = sys.argv[1]
        Server_port = int(sys.argv[2])
        break
    else:
        print("[-] Err: please specify server IP & Port to establish connection")
        exit()

client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client.bind(("0.0.0.0", 0))
client_ip, client_port = client.getsockname()
server_addr = (Server_ip, Server_port)

folder = './client_storage'
os.makedirs(folder, exist_ok=True)


def recv_text_response(timeout=2, bufsize=1024):
    """Wait for a decodable text response from the server, silently
    discarding any stray/binary packets left over from a previous
    (aborted/timed-out) transfer instead of crashing on decode()."""
    client.settimeout(timeout)
    deadline = time.time() + timeout
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise socket.timeout("no valid response from server")
        client.settimeout(remaining)
        resp, addr = client.recvfrom(bufsize)
        if addr != server_addr:
            continue
        try:
            return resp.decode(), addr
        except UnicodeDecodeError:
            # stray/binary packet from a previous session - ignore and keep waiting
            continue


def prog_indicator(progress):
    count = 0
    bar = ""
    while count < int(progress / 2):
        bar += "|"
        count += 1
    return bar


def stop_wait_send(seq_num, packet):
    retry = 0
    while retry < 10:
        try:
            client.sendto(packet, server_addr)
            client.settimeout(0.5)
            ack, addr = client.recvfrom(4)
            received_seq = struct.unpack("!I", ack)[0]

            if addr != server_addr:
                continue
            if received_seq == seq_num:
                return 1
            else:
                retry += 1
        except socket.timeout:
            retry += 1
        except Exception as err:
            retry += 1
            print(f"[-] ERR: {err}")
    if retry == 1:
        print(f"[-] Transfer failed: no ACK for chunk {seq_num} after 10 retries")
    return 0


def PUT(command):
    parsed_cmd = command.split(" ")
    if len(parsed_cmd) < 2:
        print("[-] ERR: Usage: PUT <filename>")
        return

    file_name = parsed_cmd[1]
    dir_ls = os.listdir(folder)
    if file_name in dir_ls:
        file_path = os.path.join(folder, file_name)
        file_size = os.path.getsize(file_path)
    else:
        print("[-] ERR: File doesn't Exist")
        return

    total_chunk = (file_size // chunk_size) if file_size > 0 else 0
    header = f"PUT|{file_name}|{file_size}|{total_chunk}"

    try:
        client.sendto(header.encode(), server_addr)
        resp_str, addr = recv_text_response(2)
        if resp_str.startswith("ERROR"):
            print(f"[-] ERR: {resp_str}")
            return
    except Exception as err:
        print(f"[-] ERR: Handshake failed: {err}")
        return

    seq_num = 0
    bytes_sent = 0
    try:
        with open(file_path, "rb") as readfile:
            while True:
                data = readfile.read(chunk_size)
                if not data:
                    break

                next_bytes_sent = bytes_sent + len(data)
                is_last = 1 if next_bytes_sent >= file_size else 0
                data_len = len(data)

                pkt_header = struct.pack("!IBH", seq_num, is_last, data_len)
                packet = pkt_header + data

                status = stop_wait_send(seq_num, packet)
                if status == 1:
                    bytes_sent = next_bytes_sent
                    seq_num += 1
                    prog = int(bytes_sent / file_size * 100) if file_size else 100
                    print(f"\rProgress: {prog_indicator(prog)} {prog}%", end="")
                else:
                    print("\n[-] ERR: server not responding, transfer aborted")
                    return

        with open(file_path, "rb") as readfile:
            client_hash = hashlib.md5(readfile.read()).hexdigest()

        client.sendto(f"DONE|{client_hash}".encode(), server_addr)
        client.settimeout(2)
        ver_status, addr = client.recvfrom(1024)
        print(f"\n[+] Response from server: HASH {ver_status.decode()}")

    except Exception as err:
        print(f"\n[-] ERR: {err}")


def GET(command):
    parsed_cmd = command.split(" ")
    if len(parsed_cmd) < 2:
        print("[-] ERR: Usage: GET <filename>")
        return

    num=0
    file_name = parsed_cmd[1]
    name = file_name
    while True:
        if name in os.listdir(folder):
            num += 1
            name_list = file_name.split('.')
            name = f'{name_list[0]}({num}).{name_list[1]}'
        else:
            file_path = os.path.join(folder, name)
            if(name != file_name):
                print(f"[-] '{file_name}' already exists, saving as '{name}'")
            break

    try:
        client.sendto(f"GET|{file_name}|0".encode(), server_addr)
        resp_str, addr = recv_text_response(2)
    except Exception as err:
        print(f"[-] ERR: Handshake failed: {err}")
        return

    if resp_str.startswith("ERR") or resp_str.startswith("ERROR"):
        print(f"[-] {resp_str}")
        return

    try:
        _, file_size_s, total_chunk_s = resp_str.split("|")
        file_size = int(file_size_s)
    except Exception:
        print(f"[-] ERR: Unexpected response: {resp_str}")
        return

    expected_seq = 0
    bytes_rcvd = 0
    try:
        with open(file_path, "wb") as write_file:
            while bytes_rcvd < file_size:
                try:
                    client.settimeout(0.5)
                    packet, addr = client.recvfrom(1024)
                except socket.timeout:
                    print("\n[-] ERR: timed out waiting for data")
                    return
                if addr != server_addr:
                    continue

                seq_num, is_last, data_len = struct.unpack("!IBH", packet[:7])
                file_data = packet[7:7 + data_len]
                ack_packet = struct.pack("!I", seq_num)

                if seq_num == expected_seq:
                    client.sendto(ack_packet, server_addr)
                    write_file.write(file_data)
                    bytes_rcvd += data_len
                    expected_seq += 1
                    prog = int(bytes_rcvd / file_size * 100) if file_size else 100
                    print(f"\rProgress: {prog_indicator(prog)} {prog}%", end="")
                elif seq_num < expected_seq:
                    client.sendto(ack_packet, server_addr)
                else:
                    continue

        try:
            client.settimeout(2)
            fin_msg, addr = client.recvfrom(1024)
            msg, server_hash = fin_msg.decode().split("|")
            if msg == "DONE":
                with open(file_path, "rb") as readfile:
                    client_hash = hashlib.md5(readfile.read()).hexdigest()

                print(f"[+] CLIENT HASH: {client_hash}")
                print(f"[+] SERVER HASH: {server_hash}")
                if client_hash == server_hash:
                    print("\n[+] Hash Matched: file received successfully")
                else:
                    print("\n[-] Hash Mismatched: received corrupted file")
        except socket.timeout:
            print("\n[+] File received (no server confirmation received)")

    except Exception as err:
        print(f"\n[-] ERR: {err}")


def LIST(command):
    if command == "LIST":
        try:
            client.sendto("LIST".encode(), server_addr)
            data_str, addr = recv_text_response(2, bufsize=4096)
        except Exception as err:
            print(f"[-] ERR: {err}")
            return

        if data_str.startswith("ERROR"):
            print(f"[-] {data_str}")
            return

        parts = data_str.split("|")
        print(f"Total files: {parts[1]}")
        for f in parts[2:]:
                print(f"{f} Bytes")
    else:
        print("[-] ERR: Invalid Command!!!")


while True:
    usr_command = input(f"(@{client_port})#>>$ ")
    if usr_command.startswith("QUIT"):
        break
    elif usr_command.startswith("HELP"):
        print("\n1.PUT: PUT <filename>\n2.GET: GET <filename>\n3.LIST: LIST\n4.QUIT: QUIT\n5.CLEAR: CLEAR\n")
    elif usr_command.startswith("PUT"):
        PUT(usr_command)
    elif usr_command.startswith("GET"):
        GET(usr_command)
    elif usr_command.startswith("CLEAR"):
        os.system('cls' if os.name == 'nt' else 'clear')
    else:
        LIST(usr_command)

client.close()