from peer import *
from peerconnection import *

import time

"""
These are the names of the supported messages
"""
GET_PEER_NAME = "NAME"
GET_PEER_LIST = "LIST"
ADD_PEER = "ADDP"
SEND_ERROR = "ERRO"
GET_DATA = "GETD"
PUT_DATA = "PUTD"
DELETE_DATA = "DELD"
REPLY = "REPL"

class KVStorageManager(Peer):

    def __init__( self, my_id, server_port, ip_tracker=None, port_tracker=None ):
        print 'Initializing KVStorageManager'
        Peer.__init__(self, my_id, server_port, ip_tracker, port_tracker)

        self.addRestoreDisplay( self.__restoreDisplay )
        self.addRequestHandler(GET_PEER_NAME, self.__getPeerName, 2, True)
        self.addRequestHandler(REPLY, self.__reply, 1, False)
        self.addRequestHandler(GET_PEER_LIST, self.__getPeerList, 2, True)
        self.addRequestHandler(ADD_PEER, self.__addPeer, 2, True)
        #self.addRequestHandler(SEND_ERROR, self.__sendError, 0)
        #self.addRequestHandler(GET_DATA, self.__getData, 0)
        #self.addRequestHandler(PUT_DATA, self.__putData, 0)
        #self.addRequestHandler(DELETE_DATA, self.__delData, 0)
        self.__buildPeersTable2(ip_tracker, port_tracker, 5)

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
     NAME: __reply
     DESCRIPTION: replies the peer id
     PARAMETERS:
    ------------------------------------"""
    def __reply( self, peer_connection, msg ):
        if self.debug:
            print 'Received {%s}'%(msg)

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
     NAME: __readCommandLine
     DESCRIPTION:
     PARAMETERS:
    ------------------------------------"""
    def __readCommandLine(self, verbose=False):
        self.restore_display()
        command = raw_input();
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
    NAME: __validateCommand
    DESCRIPTION: determines if the command 
                 provided is valid
    PARAMETERS: 
          command (IN) Command to validate
    ------------------------------------"""            
    def __validateCommand(self, command):
        num_arguments = len(command)
        assert num_arguments > 1
        command_name = command[1].upper()

        if command_name not in self.handlers:
            print 'Command not implemented'
            return (False, False)

        handler = self.handlers[command_name]
        if handler[1] == num_arguments:
            return (True, handler[2])
        else:
            print 'Invalid number of arguments'
            return (False, False)

        
    """------------------------------------
     NAME: __restoreDisplay
     DESCRIPTION: Manages the command line
     PARAMETERS:
    ------------------------------------"""
    def __restoreDisplay( self ):
        print '\n\nType a command: '


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
        while not self.shutdown:
            try:
                arguments = self.__readCommandLine()
                if not arguments:
                    print "No argument provided"
                    continue
                        
                if arguments[0].upper() == "BYE":
                    print 'Shutting down threads...'
                    self.shutdown = True
                    continue
                    
                if arguments[0].upper() == "MY_LIST":
                    print self.peer_list
                    continue

                if arguments[0].upper() == "MY_NAME":
                    print 'I am %s: %s, %d' % (self.my_id, self.server_host, int(self.server_port))
                    continue

                if arguments[0].upper() == 'HELP':
                    print 'Valid commands are:'
                    print '  MY_LIST: list my peers'
                    print '  MY_NAME: shows my id'
                    print '  BYE: quit this peer'
                    print ''
                    print 'Valid queries are:'
                    print '  <peer_id> LIST: return remote peer list'
                    print '  <peer_id> NAME: return remote peer name'
                    continue

                (found, wait_reply) = self.__validateCommand(arguments)
                if not found:
                    print "Error validating command"
                    continue
                            
                #send to: (peerId, msg, data)
                data = ""
                num_args = len(arguments)
                if num_args > 2: data = arguments[2]
                rc = self.sendToPeer(arguments[0], 
                                     arguments[1], 
                                     data, 
                                     wait_reply)
                if not rc:
                    print "Could not process command"
                else:
                    print rc

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
