from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
import argparse
import random
import signal
import time
import threading
import xmlrpc.client
import datetime
import hashlib

def getHash(data):
    return hashlib.sha256(data).hexdigest()


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


# A simple ping, returns true
def ping():
    """A simple ping method"""
    print("Ping()")
    return True

# Gets a block, given a specific hash value
blockStore = {}
def getblock(h):
    """Gets a block"""
    print("GetBlock(" + h + ")")
    #blockData = bytes(4)
    return blockStore[h]

# Puts a block
def putblock(b):
    """Puts a block"""
    print("PutBlock()")
    b = b.data
    h = getHash(b)
    blockStore[h] = b
    return True

# Given a list of blocks, return the subset that are on this server
def hasblocks(blocklist):
    """Determines which blocks are on this server"""
    print("HasBlocks()")
    blocklist = [b for b in blocklist if b in blockStore.values()]
    return blocklist


# Retrieves the server's FileInfoMap
def getfileinfomap():
    """Gets the fileinfo map"""
    print("GetFileInfoMap()")

    if isLeader():
        commit = False
        while not commit:
            ack_count = 1
            for s in serverlist:
                client = xmlrpc.client.ServerProxy('http://' + s)
                if not client.surfstore.isCrashed():
                    ack_count += 1
                    if ack_count * 2 > maxnum:
                        # majority consent
                        commit = True
                        break
    else:
        raise Exception("not leader")
    return fileInfoMap


# Update a file's fileinfo entry
def updatefile(filename, version, blocklist):
    """Updates a file's fileinfo entry"""
    print("UpdateFile()")
    global fileInfoMap
    global servernum
    global currentTerm
    global serverlist
    global commitIndex
    global lastApplied
    global matchIndex
    global crashedFollowers

    if isLeader():
        commit = False
        while not commit:
            ack_count = 1
            for s in serverlist:
                client = xmlrpc.client.ServerProxy('http://' + s)
                if not client.surfstore.isCrashed():
                    ack_count += 1
                    print("ack_count:" + str(ack_count))
                    if ack_count * 2 > maxnum:
                        # majority consent
                        commit = True
                        break
        if commit:
            commitIndex += 1
            if filename in fileInfoMap.keys():
                print(fileInfoMap[filename][0], version)
                if fileInfoMap[filename][0] >= version:
                    # conflict
                    return False
            myLog.append((currentTerm, (filename, version, blocklist)))
            for i, s in enumerate(serverlist):
                client = xmlrpc.client.ServerProxy('http://' + s)
                # while not committed:
                try:
                    matchIndex[i] = client.surfstore.appendEntries(currentTerm, servernum, myLog, commitIndex)
                except:
                    print("AppendEntries: no response from server" + str(i))
            print(matchIndex, commitIndex, lastApplied)
            N = sorted(matchIndex)[len(matchIndex)//2]
            if commitIndex < N and myLog[N][0] == currentTerm:
                commitIndex = N
            if commitIndex > lastApplied:
                for i in range(lastApplied+1, commitIndex+1):
                    filename, version, blocklist = myLog[i][1]
                    fileInfoMap[filename] = [version, blocklist]
                lastApplied = commitIndex
            print(matchIndex, commitIndex, lastApplied, myLog, fileInfoMap)
    else:
        raise Exception("not leader")
    return True

# 3 states: follower=0, candidate=1, leader=2
# if a follower don't hear from the leader, then it becomes a candidate
# the candidate then requests votes from other nodes
# the candidate becomes the leader if it gets votes from a majority of nodes
class TimeoutError(Exception):
    pass


def random_election_timeout(low=700, high=1000):
    return random.randint(low, high) / 1000


def handler(signum, frame):
    print("handler: time out")
    print("current state: ", state)
    # print(datetime.datetime.now())
    raise TimeoutError()


def state_machine():
    global state
    state = 0
    print("begin")
    signal.signal(signal.SIGALRM, handler)
    while True:
        if state == 0:  # follower
            print("follower: term ", currentTerm)
            follower()
        elif state == 1:  # candidate
            print("candidate: term ", currentTerm + 1)
            candidate()
        elif state == 2:  # leader
            print("leader: term ", currentTerm)
            leader()
        else:
            pass  # print("I'm dead")


def leader():
    global state
    global currentTerm
    global votedFor
    global serverlist
    global commitIndex
    global lastApplied
    global matchIndex
    global myLog
    global crashedFollowers

    signal.setitimer(signal.ITIMER_REAL, 0)
    while True:
        if state != 2:
            break
        for i, s in enumerate(serverlist):
            client = xmlrpc.client.ServerProxy('http://' + s)
            if state != 2:
                break
            try:
                matchIndex[i] = client.surfstore.appendEntries(currentTerm, servernum, myLog, commitIndex)
            except:
                print("AppendEntris: No response from some server")
        time.sleep(0.1)


def candidate():
    global state
    global currentTerm
    global votedFor
    signal.setitimer(signal.ITIMER_REAL, 0)
    currentTerm += 1
    votedFor = servernum
    voteCount = 1
    for s in serverlist:
        client = xmlrpc.client.ServerProxy('http://' + s)
        if state != 1:
            break
        print("candidate: requesting vote...")
        try:
            if client.surfstore.requestVote(servernum, currentTerm, commitIndex):
                voteCount += 1
                print("candidate: Vote granted")
            else:
                print("candidate: Vote denied")
        except:
            print("RequestVote: No response from some server")
    if voteCount * 2 > maxnum:
        state = 2
        return True
    time.sleep(random_election_timeout())
    return False


def follower():
    global state
    global currentTerm
    global votedFor
    signal.setitimer(signal.ITIMER_REAL, random_election_timeout())
    try:
        while state == 0:
            pass
    except TimeoutError as exc:
        print("follower: election timeout!!!")
        print("follower: becoming candidate")
        state = 1
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
    # signal.alarm(0)


# election timeout: the amount of time a follower waits until becoming a candidate (randomized to be beteern 150ms and 300ms)
# once the election timeout, a follower becomes a candidate and votes for itself, and send out RequestVote message to other nodes.
# if the receiving node hasn't voted yet in this term then it votes for the candidate and the node resets its election timeout.

# each change is added as an entry in the node's log
# the leader begins sending out AppendEntries messages to its followers.
# these messages are sent in intervals specified by the heartbeat timeout
# followers then respond to each AppendEntries message.

# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    print("IsLeader()")
    if state == 2:
        return True
    return False


# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    global state
    """Crashes this metadata store"""
    state = -1
    signal.setitimer(signal.ITIMER_REAL, 0)
    print("Crash()")
    return True


# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    global state
    """Restores this metadata store"""
    state = 0
    print("Restore()")
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    if state == -1:
        return True
    return False


# Requests vote from this server to become the leader
def requestVote(serverid, term, candidate_commit_index):
    """Requests vote to be the leader"""
    global state
    global currentTerm
    global votedFor
    print("Vote requested from ", serverid, " currentTerm ", currentTerm, " term ", term)
    if state == -1:
        return False
    if term < currentTerm:
        return False
    elif votedFor != None and votedFor != serverid and term == currentTerm:
        return False
    currentTerm = term
    if candidate_commit_index < commitIndex:
        return False
    signal.setitimer(signal.ITIMER_REAL, random_election_timeout())
    print("decide to vote, become follower")
    votedFor = serverid
    state = 0
    return True


# Updates fileinfomap
def appendEntries(term, leaderID, entries, leaderCommit):
    """Updates fileinfomap to match that of the leader"""
    global state
    global currentTerm
    global votedFor
    global fileInfoMap
    global commitIndex
    global lastApplied
    global myLog

    if state == -1:
        return lastApplied
    if term < currentTerm:
        return lastApplied
    signal.setitimer(signal.ITIMER_REAL, random_election_timeout())
    # TODO
    # print(datetime.datetime.now())
    state = 0
    currentTerm = term
    myLog = entries
    # print(myLog, entries)
    commitIndex = leaderCommit
    # print(commitIndex, lastApplied, myLog, fileInfoMap)
    if commitIndex > lastApplied:
        for i in range(lastApplied + 1, commitIndex + 1):
            filename, version, blocklist = myLog[i][1]
            fileInfoMap[filename] = [version, blocklist]
        lastApplied = commitIndex
    return commitIndex


def tester_getversion(filename):
    return fileInfoMap[filename][0]


# Reads the config file and return host, port and store list of other servers
def readconfig(config, servernum):
    """Reads cofig file"""
    fd = open(config, 'r')
    l = fd.readline()

    maxnum = int(l.strip().split(' ')[1])

    if servernum >= maxnum or servernum < 0:
        raise Exception('Server number out of range.')

    d = fd.read()
    d = d.splitlines()

    for i in range(len(d)):
        hostport = d[i].strip().split(' ')[1]
        if i == servernum:
            host = hostport.split(':')[0]
            port = int(hostport.split(':')[1])

        else:
            serverlist.append(hostport)

    return maxnum, host, port


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="SurfStore server")
        parser.add_argument('config', help='path to config file')
        parser.add_argument('servernum', type=int, help='server number')
        args = parser.parse_args()

        config = args.config
        servernum = args.servernum

        # server list has list of other servers
        serverlist = []

        # maxnum is maximum number of servers
        maxnum, host, port = readconfig(config, servernum)

        print(serverlist)

        '''clientlist = []
        for s in serverlist:
            clientlist.append(xmlrpc.client.ServerProxy('http://' + s))'''

        # hashmap = dict();

        fileInfoMap = dict()
        myLog = [(-1)]
        currentTerm = 0
        votedFor = None
        state = 0
        nextIndex = [1] * (maxnum - 1)
        matchIndex = [0] * (maxnum - 1)
        lastApplied = 0
        commitIndex = 0
        crashedFollowers = set()

        print("Attempting to start XML-RPC Server...")
        print(host, port)
        server = threadedXMLRPCServer((host, port), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping, "surfstore.ping")
        server.register_function(getblock, "surfstore.getblock")
        server.register_function(putblock, "surfstore.putblock")
        server.register_function(hasblocks, "surfstore.hasblocks")
        server.register_function(getfileinfomap, "surfstore.getfileinfomap")
        server.register_function(updatefile, "surfstore.updatefile")
        # Project 3 APIs
        server.register_function(isLeader, "surfstore.isLeader")
        server.register_function(crash, "surfstore.crash")
        server.register_function(restore, "surfstore.restore")
        server.register_function(isCrashed, "surfstore.isCrashed")
        server.register_function(requestVote, "surfstore.requestVote")
        server.register_function(appendEntries, "surfstore.appendEntries")
        server.register_function(tester_getversion, "surfstore.tester_getversion")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")

        thr = threading.Thread(target=server.serve_forever)
        thr.daemon = True
        thr.start()

        # time.sleep(10)

        state_machine()

        # thr.join()
    except Exception as e:
        print("Server: " + str(e))
        print(state)
