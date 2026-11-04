# UDPFTPserver.py

import threading
import sys
import socket
import struct
import os
import time
import hashlib

HOST = "0.0.0.0"
PORT = ''
client_list = []

folder = './server_storage'
os.makedirs(folder, exist_ok=True)


def get_client(client_addr):
    for item in client_list:
        if item['socket'] == client_addr:
            return item
    return None


def stop_wait(client, seq_num, packet, client_addr):

    retry = 0
    while retry < 10:
        try:
            server.sendto(packet, client_addr)
            t_start = time.time()
            while len(client['ack_buffer']) == 0 and (time.time() - t_start) < 0.5:
                continue

            if len(client['ack_buffer']) == 0:
                retry += 1
                print(f"[{client_addr}] [-] Packet {seq_num} retransmitting (timeout)")
                continue

            ack, addr = client['ack_buffer'].pop(0)
            received_seq = struct.unpack("!I", ack)[0]

            if addr != client_addr:
                continue
            if received_seq == seq_num:
                print(f"[{client_addr}] [+] Packet {seq_num} send successfully")
                return 1
            else:
                print(f"[{client_addr}] [-] Packet {seq_num} retransmitting")
                retry += 1
        except Exception as err:
            retry += 1
            print(f"[{client_addr}] [-] ERR: {err}")
    if retry == 10:
        print(f"[{client_addr}] [-] Transfer failed: no ACK for chunk {seq_num} after 10 retries")
    return 0


def get_file_path(file_name, socket):
    num = 0
    name = file_name
    while True:
        if name in os.listdir(folder):
            num += 1
            name_list = file_name.split('.')
            name = f'{name_list[0]}({num}).{name_list[1]}'
        else:
            file_path = os.path.join(folder, name)
            if name != file_name:
                print(f"[{socket}] [-] '{file_name}' already exists, saving as '{name}'")
            break
    return file_path


def serve_put(packet, client_addr):
    client = get_client(client_addr)
    handshake = (packet.decode()).split("|")
    cmd, file_name, file_size, total_chunk = handshake
    client['file_name'] = file_name
    client['file_size'] = int(file_size)
    client['file_path'] = get_file_path(file_name, client_addr)
    client['file'] = open(client['file_path'], 'wb')

    try:
        hand_ack = 'READY|0'
        server.sendto(hand_ack.encode(), client_addr)
    except Exception as err:
        print(f"[{client['socket']}] [-] ERR: Handshake Error: {err}")
        hand_err = f'ERROR|{err}'
        server.sendto(hand_err.encode(), client_addr)

    print(f"\n[{client['socket']}] Client want to send a file")
    print(f"[{client['socket']}] File Name: {file_name} File Size: {file_size}")
    print(f"[{client['socket']}] File receiving initiated...")


def serve_get(packet, client_addr):
    client = get_client(client_addr)
    cmd, file_name, start = (packet.decode()).split("|")

    client['file_name'] = file_name
    chunk_size = 1000

    print(f"\n[{client_addr}] Client Requested for file: {file_name}")
    print(f"[{client_addr}] File search initiated...")

    try:
        dir_ls = os.listdir(folder)
        if file_name in dir_ls:
            file_path = os.path.join(folder, file_name)
            file_size = os.path.getsize(file_path)
            total_chunk = file_size // chunk_size
        else:
            print(f"[{client_addr}] [-] ERR: Requested file doesn't Exist")
            err_ack = f"ERR: File Not Found"
            server.sendto(err_ack.encode(), client_addr)
            return

        hand_ack = f'READY|{file_size}|{total_chunk}'
        server.sendto(hand_ack.encode(), client_addr)

    except Exception as err:
        print(f"[{client_addr}] [-] ERR: Handshake Error: {err}")
        hand_err = f'ERROR|{err}'
        server.sendto(hand_err.encode(), client_addr)

    try:
        seq_num = 0
        bytes_send = 0
        with open(file_path, 'rb') as read_file:
            while True:
                data = read_file.read(chunk_size)
                if not data:
                    break
                is_last = 1 if (seq_num == total_chunk - 1) else 0
                data_len = len(data)

                header = struct.pack("!IBH", seq_num, is_last, data_len)
                out_packet = header + data

                status = stop_wait(client, seq_num, out_packet, client_addr)

                if status == 1:
                    bytes_send += data_len
                    seq_num += 1
                else:
                    print(f"[{client_addr}] Client didn't replied")
                    break
        if bytes_send == file_size:
            with open(file_path, "rb") as readfile:
                file_content = readfile.read()
                server_hash = hashlib.md5(file_content).hexdigest()
                fin_msg = f"DONE|{server_hash}"
                server.sendto(fin_msg.encode(), client_addr)

            print(f"[{client_addr}] [+] Data Sent: {file_size} Bytes")
            print(f"[{client_addr}] [+] File Sent Successfully")
        else:
            print(f"[{client_addr}] Unable to send file, Client not responding")

    except Exception as err:
        print(f"[{client_addr}] [-] Err: {err}")
        err = f'ERROR|{err}'
        server.sendto(err.encode(), client_addr)

    # GET transfer is finished (success or failure) - drop the session.
    client_list.remove(client)


def serve_list(packet, client_addr):
    print(f"\n[{client_addr}] Client asked for file list")
    try:
        list_ = os.listdir(folder)
        file_list = f'OK|{str(len(list_))}'

        for f in list_:
            file_path = os.path.join(folder, f)
            file_size = os.path.getsize(file_path)
            file_list += str(f'|{f}  {file_size}')

        server.sendto(file_list.encode(), client_addr)
        print(f"[{client_addr}] [+] File list sent successfully")
    except Exception as err:
        print(f"[{client_addr}] [-] Err: {err}")
        err = f'ERROR|{err}'
        server.sendto(err.encode(), client_addr)


def serve_done(packet, client_addr):
    client = get_client(client_addr)
    try:
        msglist = (packet.decode()).split('|')
        msg, client_hash = msglist

        with open(client['file_path'], "rb") as readfile:
            file_content = readfile.read()
            server_hash = hashlib.md5(file_content).hexdigest()

            print(f"[{client_addr}] [+] CLIENT HASH: {client_hash}")
            print(f"[{client_addr}] [+] SERVER HASH: {server_hash}")

            if server_hash == client_hash:
                server.sendto('VERIFIED'.encode(), client_addr)
                print(f"[{client_addr}] [+] Hash Matched: file received successfully")
            else:
                server.sendto('MISMATCH'.encode(), client_addr)
                print(f"[{client_addr}] [-] Hash Mismatched: received currupted file")
    except Exception as err:
        print(f"[{client_addr}] [-] ERR: {err}")
    finally:
        # PUT transfer is finished (verified or not) - drop the session.
        if client in client_list:
            client_list.remove(client)


def serve_packet(packet, client_addr):
    client = get_client(client_addr)

    try:
        seq_num, is_last, data_len = struct.unpack("!IBH", packet[:7])
        file_data = packet[7:7 + data_len]
        ack_packet = struct.pack("!I", seq_num)
        not_data_packet = False

    except struct.error:
        not_data_packet = True

    if not_data_packet:
        client['ack_buffer'].append((packet, client_addr))
    else:
        if client['expected_seq'] == seq_num:
            server.sendto(ack_packet, client_addr)
            print(f"[{client['socket']}] [+] Packet {seq_num} received successfully")
            client['file'].write(file_data)
            client['expected_seq'] += 1

        elif client['expected_seq'] > seq_num:
            server.sendto(ack_packet, client_addr)

        elif client['expected_seq'] < seq_num:
            print(f"[{client['socket']}] [-] Out of Order packet received")

        else:
            print(f"[{client['socket']}] [-]ERR: Sequence Number must be an integer")

        if is_last == 1:
            print(f"[{client['socket']}] [+] Data Received: {client['file_size']} Bytes")
            client['file'].close()


# here server start
while True:
    if len(sys.argv) > 1:
        PORT = int(sys.argv[1])
        break
    else:
        print("[-] Err: please specify port to start server")
        exit()

try:
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    print(f"UDP server started {HOST}:{PORT}")

    while True:
        packet, client_addr = server.recvfrom(1024)
        if packet.decode(errors="ignore").startswith("PUT"):
            if not any(item['socket'] == client_addr for item in client_list):
                client = {
                    'socket': client_addr,
                    'expected_seq': 0,
                    'file_name': None,
                    'file_size': None,
                    'file_path': None,
                    'file': None,
                    'ack_buffer': []
                }
                client_list.append(client)
            serve_put(packet, client_addr)

        elif packet.decode(errors="ignore").startswith("GET"):
            if not any(item['socket'] == client_addr for item in client_list):
                client = {
                    'socket': client_addr,
                    'expected_seq': 0,
                    'file_name': None,
                    'file_size': None,
                    'file_path': None,
                    'file': None,
                    'ack_buffer': []
                }
                client_list.append(client)
            threading.Thread(target=serve_get, args=(packet, client_addr), daemon=True).start()

        elif packet.decode(errors="ignore").startswith("LIST"):
            if not any(item['socket'] == client_addr for item in client_list):
                client = {
                    'socket': client_addr,
                    'expected_seq': 0,
                    'file_name': None,
                    'file_size': None,
                    'file_path': None,
                    'file': None,
                    'ack_buffer': []
                }
                client_list.append(client)
            serve_list(packet, client_addr)

        elif packet.decode(errors="ignore").startswith("DONE"):
            if not any(item['socket'] == client_addr for item in client_list):
                print(f"[-] UNKNOW CLIENT")
                continue
            serve_done(packet, client_addr)

        else:
            if not any(item['socket'] == client_addr for item in client_list):
                print(f"[-] UNKNOW CLIENT")
                continue
            serve_packet(packet, client_addr)

except Exception as err:
    print(f"[-] ERR: {err}")
    exit()

finally:
    server.close()