from peer import *
from peerconnection import *

import time

"""
These are the names of the supported messages
"""
GET_PEER_NAME = "NAME"
GET_PEER_LIST = "LIST"
ADD_PEER = "ADDP"

GET_DATA = "GETD"
PUT_DATA = "PUTD"
DEL_DATA = "DELD"
DEL_ALL_DATA = "DELA"
GET_ALL_DATA = "GETA"
REPLY = "REPL"
SEND_ERROR = "ERRO"

class KVStorageManager(Peer):

    def __init__( self, my_id, server_port, ip_tracker=None, port_tracker=None ):
        print 'Initializing KVStorageManager'
        Peer.__init__(self, my_id, server_port, ip_tracker, port_tracker)

        self.addRequestHandler(GET_PEER_NAME, self.__getPeerName)
        self.addRequestHandler(GET_PEER_LIST, self.__getPeerList)
        self.addRequestHandler(ADD_PEER, self.__addPeer)

        self.addRequestHandler(GET_DATA, self.__getData)
        self.addRequestHandler(PUT_DATA, self.__putData)
        self.addRequestHandler(DEL_DATA, self.__delData)
        self.addRequestHandler(GET_ALL_DATA, self.__getAllData)
        self.addRequestHandler(DEL_ALL_DATA, self.__delAllData)

        self.__buildPeersTable2(ip_tracker, port_tracker, 5)

        self.kvstore = {}

    """------------------------------------
     NAME: __buildPeersTable2
     DESCRIPTION: Builds a peer table given 
                  an initial peer
     PARAMETERS:
    ------------------------------------"""
    def __buildPeersTable2(self, remote_host=None, remote_port=None, ttl=5):

        #Do do anything for the first peer
        if remote_host is None or remote_port is None:
            return

        #We can not contain more than certain number of elements in our list
	if self.maxPeersReached() or not ttl:
	    return

	peer_id = None

	try:
	    _, peer_id = self.connectAndSend(self.my_id, remote_host, remote_port, GET_PEER_NAME, '', True)[0]

            #peer_id already in my list
            if (peer_id in self.getPeerIds()):
                return

	    if self.debug:
                print 'Exchanging peers with %s' %(peer_id)

	    resp = self.connectAndSend(self.my_id, remote_host, remote_port, ADD_PEER, 
					'%s %s %d' % (self.my_id, 
						      self.server_host, 
						      self.server_port), True)[0]

            #response should be REPLY
	    if (resp[0] != REPLY):
		return

            #Add this peer to my list
	    self.insertPeer(peer_id, remote_host, remote_port)

            #Ask for the remote peer's list
	    resp = self.connectAndSend(self.my_id, remote_host, 
                                       remote_port, 
                                       GET_PEER_LIST, 
                                       '',
                                       True)
	    if len(resp) > 1:
		resp.reverse()
		resp.pop()    # get rid of header count reply
		while len(resp):
		    next_pid, next_host, next_port = resp.pop()[1].split()
		    if next_pid != self.my_id:
			self.__buildPeersTable2(next_host, next_port, ttl - 1)

        except:
	    traceback.print_exc()
	    self.deletePeer(peer_id)


    """------------------------------------
     NAME: __getPeerName
     DESCRIPTION: replies the peer id
     PARAMETERS:
    ------------------------------------"""
    def __getPeerName( self, peer_connection, msg ):
        if self.debug:
            print 'Replying with peer name...'
        peer_connection.sendData( REPLY, self.my_id )


    """------------------------------------
     NAME: __getPeerList
     DESCRIPTION: replies the peer id
     PARAMETERS:
    ------------------------------------"""
    def __getPeerList( self, peer_connection, data ):
        if self.debug:
            print 'Listing peers %d' % (self.numberOfPeers())
        peer_connection.sendData(REPLY, '%d' % self.numberOfPeers())
        for pid in self.getPeerIds():
            host,port = self.getPeer(pid)
            peer_connection.sendData(REPLY, '%s %s %d' % (pid, host, port))


    """------------------------------------
     NAME: __addPeer
     DESCRIPTION: 
     PARAMETERS:
    ------------------------------------"""
    def __addPeer(self, peer_connection, data):
        try:
            peer_id,host,port = data.split()

            if self.maxPeersReached():
                if self.debug:
                    print 'maxpeers %d reached: connection terminating' % (self.maxpeers)
                peer_connection.sendData(SEND_ERROR, 'Join: too many peers')
                return

            # peerid = '%s:%s' % (host,port)
            if peer_id not in self.getPeerIds() and peer_id != self.my_id:
                self.insertPeer(peer_id, host, port)
                if self.debug:
                    print 'added peer: %s' % (peer_id)
                peer_connection.sendData(REPLY, 'Join: peer added: %s' % peer_id)
                if self.debug:
                    print 'My current list: %s' % self.peer_list
            else:
                peer_connection.sendData(SEND_ERROR, 'Join: peer already inserted %s'
                                             % peer_id)
        except:
            print 'invalid insert %s: %s' % (str(peer_connection), data)
            peer_connection.sendData(SEND_ERROR, 'Join: incorrect arguments')



    """------------------------------------
     NAME: __getData
     DESCRIPTION: 
     PARAMETERS:
    ------------------------------------"""
    def __getData( self, peer_connection, msg ):
        try:
            key = msg
            if self.debug:
                print 'Searching %s locally' % key
            if key in self.kvstore:
                if self.debug:
                    print 'Found'
                peer_connection.sendData(REPLY, self.kvstore[key])
            else:
                if self.debug:
                    print 'Not found'
                peer_connection.sendData(SEND_ERROR, 'Key not here')

        except:
            if self.debug:
                print 'invalid request'
            peer_connection.sendData(SEND_ERROR, 'Invalid request')

    """------------------------------------
     NAME: __getData
     DESCRIPTION: 
     PARAMETERS:
    ------------------------------------"""
    def __getAllData( self, peer_connection, msg ):
        try:
            num_elements = len(self.kvstore)
            peer_connection.sendData(REPLY, '%d'%num_elements)

            for key in self.kvstore:
                peer_connection.sendData(REPLY, '%s %s' % (key, self.kvstore[key]))

        except:
            if self.debug:
                print 'invalid request'
            peer_connection.sendData(SEND_ERROR, 'Invalid request')

    """------------------------------------
     NAME: __putData
     DESCRIPTION: 
     PARAMETERS:
    ------------------------------------"""
    def __putData( self, peer_connection, msg ):
        try:
            key, value = msg.split()
            if key in self.kvstore:
                self.kvstore[key] = value
                peer_connection.sendData(REPLY, self.kvstore[key])
            else:
                peer_connection.sendData(SEND_ERROR, 'Key not here')
        except:
            if self.debug:
                print 'invalid request'
            peer_connection.sendData(SEND_ERROR, 'Invalid request')


    """------------------------------------
     NAME: __delData
     DESCRIPTION:
     PARAMETERS:
    ------------------------------------"""
    def __delData( self, peer_connection, msg ):
        try:
            if key in self.kvstore:
                del self.kvstore[key]
                peer_connection.sendData(REPLY, self.kvstore[key])
            else:
                peer_connection.sendData(SEND_ERROR, 'Key not here')
        except:
            if self.debug:
                print 'invalid request'
            peer_connection.sendData(SEND_ERROR, 'Invalid request')

    """------------------------------------
     NAME: __delAllData
     DESCRIPTION:
     PARAMETERS:
    ------------------------------------"""
    def __delAllData( self, peer_connection, msg ):
        try:
            for key in self.kvstore:
                del self.kvstore[key]
        except:
            if self.debug:
                print 'invalid request'


    """------------------------------------
     NAME: __doInsertKey
     DESCRIPTION:
     PARAMETERS:
    ------------------------------------"""
    def __doInsertKey(self, key, value):
        if self.debug:
            print 'Inserting {%s: %s}'%(key,value)

        if key in self.kvstore:
            self.kvstore[key] = value
            print 'Inserted {%s: %s}' % (key, value)
            return

        if self.debug:
            print 'Not found locally... Searching remotely' 

        found = False

        for peer_id in self.getPeerIds():
            resp = self.sendToPeer(peer_id,
                                 PUT_DATA, 
                                 '%s %s'%(key,value), 
                                 True)[0]
                
            if resp is None or resp[0] != REPLY:
                continue

            print 'Inserted {%s: %s}' % (key, value)
            found = True
            break

        if not found:
            if self.debug:
                print 'Not found remotely... Inserting locally'
            self.kvstore[key] = value
            print 'Inserted {%s: %s}' % (key, value)
            return


    """------------------------------------
     NAME: __doDeleteKey
     DESCRIPTION:
     PARAMETERS:
    ------------------------------------"""
    def __doDeleteKey(self, key):
        if self.debug:
            print 'Deleting {%s}'%(key)

        if key in self.kvstore:
            del self.kvstore[key]
            print 'Deleted %s' % key
            return

        if self.debug:
            print 'Not found locally... Searching remotely' 

        found = False

        for peer_id in self.getPeerIds():
            resp = self.sendToPeer(peer_id,
                                 DEL_DATA, 
                                 key, 
                                 True)[0]
                
            if resp is None or resp[0] != REPLY:
                continue

            if resp[1] != '':
                print 'Deleted %s' % key
                found = True
                break

        if not found:
            print 'Key not found'
            return

    """------------------------------------
     NAME: __doDelAllValue
     DESCRIPTION:
     PARAMETERS:
    ------------------------------------"""
    def __doDelAllValue(self):
        if self.debug:
            print 'Deleting All'

        for key in self.kvstore:
            del self.kvstore[key]

        for peer_id in self.getPeerIds():
            if self.debug:
                print 'Looking in %s' % peer_id

            resp = self.sendToPeer(peer_id,
                                 DEL_ALL_DATA, 
                                 '', False)
        print "Deleted all"


    """------------------------------------
     NAME: __doGetValue
     DESCRIPTION:
     PARAMETERS:
    ------------------------------------"""
    def __doGetValue(self, key):
        if self.debug:
            print 'Getting {%s}'%(key)

        if key in self.kvstore:
            print '{%s: %s}' % (key, self.kvstore[key])
            return

        if self.debug:
            print 'Not found locally... Searching remotely' 

        found = False

        for peer_id in self.getPeerIds():
            if self.debug:
                print 'Looking in %s' % peer_id

            resp = self.sendToPeer(peer_id,
                                 GET_DATA, 
                                 key, 
                                 True)[0]
                
            if resp is None or resp[0] != REPLY:
                continue

            if resp[1] != '':
                print '{%s: %s}' % (key, resp[1])
                found = True
                break

        if not found:
            print 'Key not found'
            return


    """------------------------------------
     NAME: __doGetAllValue
     DESCRIPTION:
     PARAMETERS:
    ------------------------------------"""
    def __doGetAllValue(self):
        if self.debug:
            print 'Getting All'

        have_keys = False

        for key in self.kvstore:
            print '{%s: %s}' % (key, self.kvstore[key])
            have_keys = True

        for peer_id in self.getPeerIds():
            if self.debug:
                print 'Looking in %s' % peer_id

            resp = self.sendToPeer(peer_id,
                                 GET_ALL_DATA, 
                                 '', 
                                 True)
                
	    if len(resp) > 1:
		resp.reverse()
		resp.pop()    # get rid of header count reply
		while len(resp):
		    key, value = resp.pop()[1].split()
		    print '{%s: %s}' % (key, value)
                    have_keys = True

        if have_keys == False:
            print "No keys"

    """------------------------------------
     NAME: __readCommandLine
     DESCRIPTION:
     PARAMETERS:
    ------------------------------------"""
    def __readCommandLine(self, verbose=False):
        #print '\n\nType a command: '
        command = raw_input('\n\nType a command >> ');
        tmp_arguments = command.split(" ")
        arguments = [_ for _ in tmp_arguments if _ != '']
        if verbose: print arguments
        if not arguments:
            if verbose: print "No arguments supplied"
            return None
        else:
            if verbose: print arguments
            return arguments


    """------------------------------------
     NAME: __console
     DESCRIPTION: Manages the command line
     PARAMETERS:
    ------------------------------------"""
    def __console( self ):
        print ''
        print 'Key-Value Store. By Ricardo Zavaleta'
        print 'Starting console...'
        print 'Type HELP for a list of valid commands/queries'

        #Keep reading commands
        while not self.shutdown:
            try:
                
                #Read command
                arguments = self.__readCommandLine()
                if not arguments:
                    print "No argument provided"
                    continue
                        
                num_args = len(arguments)
                data = ''

                #Find the command just received
                if arguments[0].upper() == "BYE":
                    print 'Shutting down threads...'
                    self.shutdown = True
                    continue
                    
                elif arguments[0].upper() == "MY_PEERS":
                    if len(self.peer_list) == 0:
                        print "No peers"

                    for peer_id in self.peer_list:
                        print '{ID: %s, IP: %s, Port: %d}' % (peer_id, self.peer_list[peer_id][0], self.peer_list[peer_id][1] )
                    continue

                elif arguments[0].upper() == "MY_NAME":
                    print '{ID: %s,  IP: %s, Port: %d}' % (self.my_id, self.server_host, int(self.server_port))
                    continue

                elif arguments[0].upper() == "MY_STORE":
                    if len(self.kvstore) == 0:
                        print "No keys"

                    for key in self.kvstore:
                        print '{%s: %s}' % (key, self.kvstore[key])
                    continue

                elif arguments[0].upper() == 'HELP':
                    print 'Valid commands are:'
                    print '  MY_PEERS: list my peers'
                    print '  MY_NAME: shows my id'
                    print '  MY_STORE: shows my stored key-value pairs'
                    print '  BYE: quit this peer'
                    print '  INSERT <key> <value>'
                    print '  DELETE <key>'
                    print '  GET <key'
                    print '  GET_ALL'
                    print '  DELETE_ALL'
                    print ''
                    #print 'Valid queries are:'
                    #print '  <peer_id> LIST: return remote peer list'
                    #print '  <peer_id> NAME: return remote peer name'
                    continue

                elif arguments[0].upper() == 'INSERT':
                    if num_args != 3:
                        print 'Invalid number of arguments'
                        print 'Usage: INSERT <key> <value>'
                        continue

                    self.__doInsertKey(arguments[1], arguments[2])
                    continue
                
                elif arguments[0].upper() == 'DELETE':
                    if num_args != 2:
                        print 'Invalid number of arguments'
                        print 'Usage: DELETE <key>'
                        continue    

                    self.__doDeleteKey(arguments[1])
                    continue

                elif arguments[0].upper() == 'GET':
                    if num_args != 2:
                        print 'Invalid number of arguments'
                        print 'Usage: GET <key>'
                        continue                  
                        
                    self.__doGetValue(arguments[1])
                    continue

                elif arguments[0].upper() == 'GET_ALL':
                    if num_args != 1:
                        print 'Invalid number of arguments'
                        print 'Usage: GET_ALL'
                        continue                  
                        
                    self.__doGetAllValue()
                    continue

                elif arguments[0].upper() == 'DELETE_ALL':
                    if num_args != 1:
                        print 'Invalid number of arguments'
                        print 'Usage: DELETE_ALL'
                        continue                  
                        
                    self.__doDelAllValue()
                    continue

                else:
                    print "Invalid command"
                    continue
                    

            except KeyboardInterrupt:
                print "\nKeyboard interruption. Stopping..."
                self.shutdown = True
                continue

            except:
                traceback.print_exc()
                self.shutdown = True
                continue

    def garbageCollector( self, delay ):
        while not self.shutdown:
            delete_list = []
            if self.debug:
                print 'Running garbage collector...'
            for peer_id in self.peer_list:
                is_connected = False
                try:
                    remote_host, remote_port = self.peer_list[peer_id]
                    peer_connection = PeerConnection( self.my_id, 
                                          remote_host, 
                                          remote_port)
                    #Send dummy message
                    peer_connection.sendData('HELL', '')
                    is_connected = True
                except:
                    delete_list.append( peer_id )

                if is_connected:
                    peer_connection.close()

            for peer_id in delete_list:
                if peer_id in self.peer_list:
                    del self.peer_list[peer_id]

            time.sleep( delay )

 
    def runPeer( self ):
        print 'Starting threads...'

        thread_connection_manager = threading.Thread( target = self.connectionHandler);
        thread_connection_manager.start()

        thread_garbage_collector = threading.Thread( target = self.garbageCollector, 
                                                     args = [5] )
        thread_garbage_collector.start()
        
        self.__console()
