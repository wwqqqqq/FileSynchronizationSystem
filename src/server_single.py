from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
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
fileInfoMap = {}
def getfileinfomap():
    """Gets the fileinfo map"""
    print("GetFileInfoMap()")
    return fileInfoMap

# Update a file's fileinfo entry
def updatefile(filename, version, blocklist):
    """Updates a file's fileinfo entry"""
    print("UpdateFile()")
    fileInfo = [version, blocklist]
    if filename in fileInfoMap.keys():
        if fileInfoMap[filename][0] != version-1:
            # crash
            return False
    fileInfoMap[filename] = fileInfo
    return True

# PROJECT 3 APIs below

# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    print("IsLeader()")
    return True

# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    print("Crash()")
    return True

# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    print("Restore()")
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    return True

if __name__ == "__main__":
    try:
        print("Attempting to start XML-RPC Server...")
        server = threadedXMLRPCServer(('localhost', 8080), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping,"surfstore.ping")
        server.register_function(getblock,"surfstore.getblock")
        server.register_function(putblock,"surfstore.putblock")
        server.register_function(hasblocks,"surfstore.hasblocks")
        server.register_function(getfileinfomap,"surfstore.getfileinfomap")
        server.register_function(updatefile,"surfstore.updatefile")

        server.register_function(isLeader,"surfstore.isleader")
        server.register_function(crash,"surfstore.crash")
        server.register_function(restore,"surfstore.restore")
        server.register_function(isCrashed,"surfstore.iscrashed")
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))
