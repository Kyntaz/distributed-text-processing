#!/usr/bin/env python3

import socket
import sys
import signal

signal.signal(signal.SIGINT, lambda x, y: exit())

def sendTCP(ip, port, message):
    try:
        BUFFER_SIZE = 1024
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip, port))
        data = " "
        s.sendall(message.encode())
        MAX_ATTEMPTS = 1024
        attempts = 0
        while len(data)<12 and data[-1] != '\n':
            data += s.recv(BUFFER_SIZE).decode()
            attempts += 1
            if attempts > MAX_ATTEMPTS:
                print("Server did not respond")
                return "REP EOF"
        data = data[1:]
        parts= str.split(data, " ")
        if parts[0] == "FPT":
            s.close()
            return data
        try:
            size = eval(parts[2])
            while len(data)<size+6+len(parts[2]):
                data += s.recv(BUFFER_SIZE).decode()
            s.close()
            return data
        except:
            return data
    except socket.error:
        print("Couldn't connect socket, check port and address.")
        return ""

def getFile(name):
    f = open(name, 'r')
    text = f.read()
    f.close()
    return text

def main():
    print("User online")
    args = sys.argv
    port = getPort(args)
    ip = getIp(args)
    print("Address:", ip)
    print("Port:", port)
    while True:
        task = input()
        if task == "list":
            answer = sendTCP(ip, port, "LST\n")
            if len(answer) == 0: continue
            parts = str.split(answer)
            if parts[0] == "FPT":
                if parts[1] == "EOF":
                    print("Request cannot be answered")
                elif parts[1] == "ERR":
                    print("Poorly formulated")
                else:
                    print("The following operations are available:")
                    for i in range(eval(parts[1])):
                        print("    "+ str(i) + "." + parts[i+2])
            else:
                print("Something went wrong")
        elif task[:7] == "request":
            parts = str.split(task)
            if (len(parts) < 3):
                print("Request requires a fpt and a filename.")
                continue

            typ = parts[1]
            filename = parts[2]

            try:
                text = getFile(filename)
                answer = sendTCP(ip, port, "REQ "+typ+" "+str(len(text))+" "+text+"\n")
                parts = str.split(answer, " ")
                print("Server answered:")
                if parts[1] == "R":
                    print("File report:", parts[3])
                elif parts[1] == "F":
                    print("Processed file:", answer[3+len(parts[0]+parts[1]+parts[2]):])
                elif parts[1] == "EOF\n":
                    print("Could not execute request")
                elif parts[1] == "ERR\n":
                    print("Poorly formulated")
                else:
                    print("Bad reply")
            except IOError:
                print("Bad File")

        elif task == "exit":
            print("Terminated by user")
            return
        else:
            print("Unknown command: "+task)
            print("The following commands are available:")
            print("    list")
            print("    request <fpt> <filename>")
            print("    exit")
            print()

def getPort(args):
    for i in range(len(args)-1):
        if args[i] == "-p":
            return eval(args[i+1])
    return 58008

def getIp(args):
    for i in range(len(args)-1):
        if args[i] == "-n":
            return args[i+1] + ".ist.utl.pt"
    return socket.gethostname()

main()
