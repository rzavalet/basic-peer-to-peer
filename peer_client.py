from kv_storage_manager import *
import sys

num_arguments = len(sys.argv)
if num_arguments != 3 and num_arguments != 5:
    print 'Invalid number of arguments. Usage: '
    print '%s <peer_name> <port> [<ip_tracker> <port_tracker>]'%(sys.argv[0])
    sys.exit()

my_id = sys.argv[1]
my_port = sys.argv[2]
ip_tracker = None
port_tracker = None

if num_arguments == 5:
    ip_tracker = sys.argv[3]
    port_tracker = sys.argv[4]
    
my_peer = KVStorageManager(my_id, my_port, ip_tracker, port_tracker)
print my_peer

#Test console
my_peer.runPeer()
