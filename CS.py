#!/usr/bin/env python3

from socket import *
import socket as libsock
from select import select
import signal
import sys
import os
from threading import Thread
from random import random

def getPort(args):
	for i in range(len(args)-1):
		if args[i] == "-p":
			return eval(args[i+1])
	return 58008

def main(exit=False):
	port = getPort(sys.argv)
	workers = []

	# TCP socket for user input
	user_socket = socket(AF_INET, SOCK_STREAM)
	user_socket.bind(("", port))
	user_socket.listen(10)

	# UDP socket for server registration
	reg_socket = socket(AF_INET, SOCK_DGRAM)
	reg_socket.bind(("", port))

	try:
		while True: answer(port, workers, user_socket, reg_socket)
	except KeyboardInterrupt:
		user_socket.close()
		reg_socket.close()
		print()

def answer(port, workers, user_socket, reg_socket):
	# Wait for a connection
	#print("Waiting for connection.")
	ready = select([user_socket, reg_socket], [], [], 5)[0]

	if user_socket in ready:
		# Message from a user
		new_socket, user_address = user_socket.accept()
		#print("Message from ", user_address)

		msg = readfrom(new_socket)
		parts = str.split(msg, " ")
		if parts[0] == "LST\n":
			answer = listWorkers(workers)
			new_socket.sendall(answer.encode())
		elif parts[0] == "REQ":
			print("Message from " + str(user_address) + ": " + parts[1])
			task = parts[1]
			data = msg[len(parts[0] + parts[1] + parts[2])+3:]

			available_servers = getWorkersForTask(task, workers)
			if len(available_servers) == 0:
				#print("No workers...")
				new_socket.sendall("REP EOF\n".encode())
			else:
				t = Thread(target=distributeTask,args=(task, data, available_servers, new_socket))
				t.start()

	if reg_socket in ready:
		# Worker is trying to connect
		message, worker_address = reg_socket.recvfrom(500)
		message = message.decode()
		#print("Message: " + message)

		if message[:3] == "REG":
			regOut = registerWorker(message, workers)
			if regOut == 0:
				print("Worker registering at ", worker_address)
				reg_socket.sendto("RAK OK\n".encode(), worker_address)
			elif regOut == -1:
				print("Failed to register worker on ", worker_address)
				reg_socket.sendto("RAK NOK\n".encode(), worker_address)
			else:
				print("Protocol (syntax) error")
				reg_socket.sendto("RAK ERR\n".encode(), worker_address)
		elif message[:3] == "UNR":
			unrOut = unregisterWorker(message, workers)
			if unrOut == 0:
				print("Worker unregistering at ", worker_address)
				reg_socket.sendto("UAK OK\n".encode(), worker_address)
			elif unrOut == -1:
				print("Failed to unregister worker at ", worker_address)
				reg_socket.sendto("UAK NOK\n".encode(), worker_address)
			else:
				print("Protocol (syntax) error")
				reg_socket.sendto("UAK ERR\n".encode(), worker_address)


		else:
			reg_socket.sendto("ERR\n".encode(), worker_address)

def readfrom(s):
	BUFFER_SIZE = 1024
	data = " "
	while len(data)<12 and data[-1] != '\n':
		data += s.recv(BUFFER_SIZE).decode()
	data = data[1:]
	parts= str.split(data, " ")
	if parts[0] == "LST\n":
		return data
	size = eval(parts[2])
	while len(data)<size+6+len(parts[2]):
		data += s.recv(BUFFER_SIZE).decode()
	return data

def listWorkers(workers):
	tasks = []
	for worker in workers:
		for task in worker[1]:
			if not (task in tasks): tasks += [task]

	answer = "FPT " + str(len(tasks))
	for task in tasks:
		answer += " " + task

	return answer + "\n"

def registerWorker(msg, ws):
	parts = str.split(msg, " ")

	tasks = []
	for i, part in enumerate(parts[1:]):
		if not (part in ["WCT", "FLW", "UPP", "LOW"]):
			if len(parts) < i+2:
				print("Bad Registration")
				return -2
			address = (part, eval(parts[i+2][:-1]))
			break

		tasks += [part]

	#print(address)
	if len(tasks) == 0:
		return -1

	ws += [(address, tasks)]
	return 0

def unregisterWorker(msg, ws):
	parts = str.split(msg, " ")
	if len(parts) != 3:
		print("Bad Unregister.")
		return -2

	address = (parts[1], eval(parts[2][:-1]))
	print(address)
	for i, worker in enumerate(ws):
		if worker[0] == address:
			del ws[i]
			return 0
	return -1

FILE_COUNT = int(random() * 100000)
def distributeTask(task, data, workers, ans_socket):
	global FILE_COUNT
	FILE_COUNT += 1
	genFile(data, "input_files/{:05d}.txt".format(FILE_COUNT))
	parts = str.split(data[:-2], "\n")
	#print(parts)
	div = len(parts) // len(workers)
	rem = len(parts) % len(workers)
	sent = 0
	answer_sockets = []
	# Divide the file and send to servers.
	for i, w in enumerate(workers[:-1]):
		to_send = ''.join(parts[sent:sent+div])
		fname = "input_files/{:05d}{:03d}.txt".format(FILE_COUNT, i)
		if (len(to_send) > 0):
			genFile(to_send, fname)
		answer_sockets += [sendTask(w, task, to_send, fname)]
		if answer_sockets[-1] == -1:
			del workers[i]
			ans_socket.sendall("REP ERR\n".encode())
			ans_socket.close()
			return

		sent += div

	# Send the remainder to the last server.
	to_send = ''.join(parts[sent:sent+rem+div])
	fname = "input_files/{:05d}{:03d}.txt".format(FILE_COUNT, len(workers)-1)
	if (len(to_send) > 0):
		genFile(to_send, fname)
	answer_sockets += [sendTask(workers[-1], task, to_send, fname)]
	if answer_sockets[-1] == -1:
			del workers[len(workers)-1]
			ans_socket.sendall("REP ERR\n".encode())
			ans_socket.close()
			return

	if task == 'LOW' or task == 'UPP' or task == 'FLW': out = ''
	if task == 'WCT': out = 0
	for i, s in enumerate(answer_sockets):
		message = readAndClose(s)
		parts = str.split(message)
		headsize = len(parts[0] + parts[1] + parts[2]) + 3
		data = message[headsize:]
		genFile(data, "output_files/{:05d}{:03d}.txt".format(FILE_COUNT, i))
		if task == 'LOW' or task == 'UPP': out += data
		if task == 'FLW' and len(data) > len(out): out = data
		if task == 'WCT': out += eval(data)
	ans = ""

	if task == 'LOW' or task == 'UPP':
		ans = "REP F " + str(len(out)) + " " + out
	if task == 'FLW':
		ans = "REP R " + str(len(out)) + " " + out
	if task == 'WCT':
		ans = "REP R " + str(len(str(out))) + " " + str(out)
	genFile(str(out), "output_files/{:05d}.txt".format(FILE_COUNT))
	ans_socket.sendall((ans+'\n').encode())
	ans_socket.close()

def sendTask(w, task, data, fname):
	message = "WRQ " + task + " " + fname + " " + str(len(data)) + " " + data
	#print(message)
	try:
		sock = socket(AF_INET, SOCK_STREAM)
		sock.connect(w[0])
		sock.sendall(message.encode())
		return sock
	except libsock.error:
		print("Working server died...")
		return -1

def readAndClose(s):
	BUFFER_SIZE = 1024
	data = " "
	while len(data)<12 and data[-1] != '\n':
		data += s.recv(BUFFER_SIZE).decode()
		data = data[1:]
		parts= str.split(data, " ")
		if parts[0] == "FPT":
			s.close()
			return data
		size = eval(parts[2])
		while len(data)<size+6+len(parts[2]):
			data += s.recv(BUFFER_SIZE).decode()
		s.close()
		return data


def getWorkersForTask(task, workers):
	out = []
	for w in workers:
		if task in w[1]: out += [w]
	return out

def genFile(data, fname):
	fname = os.path.abspath(fname)
	try:
		f = open(fname, 'w')
		f.write(data)
		f.close()
	except IOError:
		print("Can't save input.")

# Run
main()
