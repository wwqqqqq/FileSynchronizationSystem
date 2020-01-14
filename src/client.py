import argparse
import xmlrpc.client
import hashlib
import time
import os
from os import listdir
from os.path import isfile, join

blockMap = {} # hash-block map

def getHash(data):
	return hashlib.sha256(data).hexdigest()

def split(filename, blocksize):
	# split a file to several blocks
	blocklist = []
	with open(filename, "rb") as f:
		while True:
			data = f.read(blocksize)
			if not data:
				break
			blocklist.append(data)
	return blocklist

def getHashList(blocklist):
	hashlist = []
	for i in blocklist:
		h = getHash(i)
		hashlist.append(h)
		blockMap[h] = i
	return hashlist


def mergeAndStore(directory, filename, hashlist):
	# merge the blocks from hashlist and store the result in filename
	with open(join(directory, filename), 'wb') as f:
		for i in hashlist:
			block = blockMap[i]
			f.write(block)
	return True

def download(client, directory, filename, hashlist):
	# download from cloud using client.getblock
	# hashlist is a list of blocks' hash value
	# update blockmap and return hashlist
	if(hashlist == 0):
		# delete file from local
		if os.path.exists(join(directory, filename)):
			os.remove(join(directory, filename))
	else:
		for h in hashlist:
			b = client.surfstore.getblock(h)
			blockMap[h] = b.data
		mergeAndStore(directory, filename, hashlist)
	return True

def upload(client, filename, version, hashlist):
	# upload blocks to cloud according to blockmap
	if(hashlist != 0):
		for h in hashlist:
			b = blockMap[h]
			client.surfstore.putblock(b)
	client.surfstore.updatefile(filename, version, hashlist)
	return True

def delete(client, filename, version):
	client.surfstore.updatefile(filename, version, 0)
	return True

def sameFile(hashlist1, hashlist2):
	if(hashlist1 == 0 and hashlist2 == 0):
		return True
	elif(hashlist1 == 0 or hashlist2 == 0):
		return False
	if(len(hashlist1) != len(hashlist2)):
		return False
	for i in range(len(hashlist1)):
		if(hashlist1[i] != hashlist2[i]):
			return False
	return True

def getAction(directory, blocksize, oldmap, fileInfoMap_cloud):
	delete_list = []
	upload_list = []
	download_list = []
	fileinfomap = {}
	print(listdir(directory))
	for filename in oldmap.keys():
		if filename not in listdir(directory) and oldmap[filename][1] != 0:
			delete_list.append(filename)
			fileinfomap[filename] = [oldmap[filename][0]+1, 0]
	for filename in listdir(directory):
		filepath = join(directory, filename)
		hashlist = getHashList(split(filepath, blocksize))
		if(filename in oldmap.keys()):
			# verify if it is modified
			if not sameFile(hashlist, oldmap[filename][1]):
				if filename not in fileInfoMap_cloud.keys() or fileInfoMap_cloud[filename][0] <= oldmap[filename][0]:
					# modified, upload to cloud
					upload_list.append(filename)
					fileinfomap[filename] = [oldmap[filename][0]+1, hashlist]
			else:
				fileinfomap[filename] = [oldmap[filename][0], hashlist]
		elif filename != "index.txt":
			# new file, upload to cloud
			if filename not in fileInfoMap_cloud.keys():
				upload_list.append(filename)
				version = 1
				fileinfomap[filename] = [version, hashlist]
	for filename in fileInfoMap_cloud.keys():
		if filename in fileinfomap.keys():
			if fileInfoMap_cloud[filename][0] > fileinfomap[filename][0]:
				download_list.append(filename)
				fileinfomap[filename] = [fileInfoMap_cloud[filename][0], fileInfoMap_cloud[filename][1]]
		else:
			download_list.append(filename)
			fileinfomap[filename] = [fileInfoMap_cloud[filename][0], fileInfoMap_cloud[filename][1]]
	return delete_list, upload_list, download_list, fileinfomap


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="SurfStore client")
	parser.add_argument('hostport', help='host:port of the server')
	parser.add_argument('basedir', help='The base directory')
	parser.add_argument('blocksize', type=int, help='Block size')
	args = parser.parse_args()

	if "http://" not in args.hostport:
		hostport = "http://" + args.hostport
	client  = xmlrpc.client.ServerProxy(hostport)
	# Test ping
	client.surfstore.ping()
	print("Ping() successful")
	while not client.surfstore.isLeader():
		print("Server is not leader")
		hostport = input("Please enter a new host:port")
		if "http://" not in args.hostport:
			hostport = "http://" + args.hostport
			client  = xmlrpc.client.ServerProxy(hostport)
			client.surfstore.ping()
			print("Ping() successful")
	basedir = args.basedir
	blocksize = args.blocksize
	fileInfoMap_local = {}
	if not os.path.exists(join(basedir, "index.txt")):
		open(join(basedir, "index.txt"), "w")
	with open(join(basedir, "index.txt")) as f:
		for line in f:
			line = line.strip()
			hashlist = line.split(' ')
			filename = hashlist[0]
			del hashlist[0]
			version = int(hashlist[0])
			del hashlist[0]
			if len(hashlist) == 1 and hashlist[0] == '0':
				hashlist = 0
			fileInfoMap_local[filename] = [version, hashlist]
	print(fileInfoMap_local)

	#while True:
	fileInfoMap_cloud = client.surfstore.getfileinfomap()
	del_list, upload_list, download_list, fileInfoMap_local = getAction(basedir, blocksize, fileInfoMap_local, fileInfoMap_cloud)
	print(del_list, upload_list, download_list)
	for filename in del_list:
		delete(client, filename, fileInfoMap_local[filename][0])
	for filename in download_list:
		download(client, basedir, filename, fileInfoMap_cloud[filename][1])
	for filename in upload_list:
		upload(client, filename, fileInfoMap_local[filename][0], fileInfoMap_local[filename][1])
	# fileInfoMap_cloud = client.surfstore.getfileinfomap()
	with open(join(basedir, "index.txt"), "w") as f:
		for i in fileInfoMap_local.keys():
			f.write(i + ' ' + str(fileInfoMap_local[i][0]))
			if fileInfoMap_local[i][1] == 0:
				f.write(' 0')
			else:
				for h in fileInfoMap_local[i][1]:
					f.write(' ' + h)
			f.write('\n')
