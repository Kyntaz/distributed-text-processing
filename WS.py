#!/usr/bin/env python3

import socket
import sys,traceback
import signal
from select import select

signal.signal(signal.SIGINT, lambda x, y: main(True))

def register(csip, csport, myport, operations):
	myip = socket.gethostbyname(socket.gethostname())
	message = "REG "
	for op in operations:
		message += (op + " ")
	message = message + myip + " " + str(myport) + "\n"
	result = sendMessageUDP(csip, csport, message)
	parts = str.split(result)
	if parts[1] == "OK":
		print("Register successful")
	else:
		print("Couldn't Register")
		exit()

def unregister(csip, csport, myport):
	myip = socket.gethostbyname(socket.gethostname())
	message = "UNR " + myip + " " + str(myport) + "\n"
	result = sendMessageUDP(csip, csport, message)
	parts = str.split(result)
	if parts[1] == "OK":
		print("\nUnregister successful")
		print("Leaving\n")
		exit()
	else:
		print("\nUnregister not accepted, leaving anyway")
		print("Leaving\n")
		exit()

def sendMessageUDP(ip, port, message):
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock.sendto(message.encode(), (ip, port))
	BUFFER_SIZE = 1024
	while True:
		ready = select([sock], [], [], 5)[0]
		data = "UNR ERR\n".encode()
		if sock in ready:
			data, addr = sock.recvfrom(BUFFER_SIZE)
		return data.decode()

user_address = None
def waitTCP(s):
	global user_address
	BUFFER_SIZE = 1024

	s.listen(10)
	ready = select([s], [], [], 5)[0]

	if s in ready:
		new_socket, user_address = s.accept()

		msg = readfrom(new_socket)
		#print(msg)
		return (msg, new_socket)
	return None

def main(exit=False):
	global user_address

	args = sys.argv
	myport = getWSPort(args)
	csport = getCSPort(args)
	csip = getCSName(args)
	operations = getOperations(args)

	if exit:
		unregister(csip, csport, myport)
	try:

		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.bind(("", myport))
		register(csip, csport, myport, operations)
	except: print("Could not create new Working Server\nLeaving"),sys.exit(0)


	while True:
		res = waitTCP(s)
		if res != None:
			message = res[0]
			sock = res[1]
			parts = str.split(message)
			if parts[0] == "WRQ":
				if parts[1] in operations:
					operation = parts[1]
					ptc = parts[2]
					size = eval(parts[3])
					headsize = len(parts[0])+len(parts[1])+len(parts[2])+len(parts[3])+4
					data = message[headsize:]
					print("Request from " + str(user_address) + ": " + operation)
					if operation == "WCT":
						words = len(str.split(data))
						message = "REP R "+ str(len(str(words))) + " " + str(words) + "\n"
						replyTCP(sock, message)
					elif operation == "FLW":
						words = str.split(data)
						maxi = 0
						maxw = ""
						for w in words:
							if len(w)>maxi:
								maxi = len(w)
								maxw = w
						message = "REP R "+ str(len(maxw)) + " " + maxw + "\n"
						replyTCP(sock, message)
					elif operation == "UPP":
						text = data.upper()
						message = "REP F "+ str(len(str(text))) + " " + text + "\n"
						replyTCP(sock, message)
					elif operation == "LOW":
						text = data.lower()
						message = "REP F "+ str(len(str(text))) + " " + text + "\n"
						replyTCP(sock, message)
					else:
						print("Undefined operation found:",operation)
						#print("Oops")
						exit()
				else:
					print("Server requested unsupported operation")
					replyTCP(sock, "REP EOF\n")
			else:
				print("Faulty CS message")
				replyTCP(sock, "REP ERR\n")

def replyTCP(sock, message):
	try:
		#print("Sending to CS:",message)
		sock.sendall(message.encode())
		sock.close()
	except socket.error:
		print("Couldn't Send to CS.")

def getOperations(args):
	ops = ()
	args = args[1:]
	for arg in args:
		if not arg in ["WCT", "UPP", "LOW", "FLW"]:
			return ops
		ops = ops + (arg,)
	return ops

def getWSPort(args):
	for i in range(len(args)-1):
		if args[i] == "-p":
			return eval(args[i+1])
	return 59000

def getCSPort(args):
	for i in range(len(args)-1):
		if args[i] == "-e":
			return eval(args[i+1])
	return 58008

def getCSName(args):
	for i in range(len(args)-1):
		if args[i] == "-n":
			return args[i+1] + ".ist.utl.pt"
	return socket.gethostname()

def readfrom(s):
	try:
		BUFFER_SIZE = 1024
		data = " "
		while len(data)<20 and data[-1] != '\n':
			data += s.recv(BUFFER_SIZE).decode()
		data = data[1:]
		parts= str.split(data, " ")
		size = eval(parts[3])
		while len(data)<size+4+len(parts[0])+len(parts[1])+len(parts[2])+len(parts[3]):
			data += s.recv(BUFFER_SIZE).decode()
		#print("Recieved from CS:", data)
		return data
	except socket.error:
		print("Couldn't get data from server.")
		return ""

main()
