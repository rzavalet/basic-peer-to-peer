#!/usr/bin/python

import socket
import threading
import traceback
from peerconnection import *

class Peer:

    """-------- CONSTRUCTOR METHODS -----------"""
    def __init__( self, my_id, server_port, ip_tracker=None, port_tracker=None ):
        self.my_id = my_id
        self.__initServerHost()
        self.server_port = int(server_port)
        self.ip_tracker = ip_tracker
        self.port_tracker = port_tracker
        self.peer_list = {}
        #self.__buildPeersTable(True)
        self.shutdown = False        
        self.handlers = {}
        self.restore_display = None
        self.max_peers = 10
        self.debug = False


    def __str__(self):
        my_string = "{ID: %s, IP: %s, Port: %d}"% (self.my_id, self.server_host, self.server_port)
        return my_string


    """-------- PRIVATE METHODS -----------"""

    """------------------------------------
     NAME: __initServerHost
     DESCRIPTION: Sets the host name
     PARAMETERS:
     ------------------------------------"""
    def __initServerHost( self ):
        s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        s.connect( ("www.google.com", 80) )
        self.server_host = s.getsockname()[0]
        s.close()

    """------------------------------------
     NAME: __makeServerSocket
     DESCRIPTION: Creates a server socket
     PARAMETERS: Port to bind the port to
    ------------------------------------"""
    def __makeServerSocket( self, port ):
        s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
        s.setsockopt( socket.SOL_SOCKET, socket.SO_REUSEADDR, 1 )
        s.bind( ('', port) )
        s.listen( 5 )
        return s

    """------------------------------------
     NAME: __addRequestHandler
     DESCRIPTION: 
     PARAMETERS: 
    ------------------------------------"""
    def addRequestHandler( self, msg_type, handler):
        assert len(msg_type) == 4
        self.handlers[msg_type] = handler


    """------------------------------------
     NAME: __handlePeer
     DESCRIPTION: Receives a message from peer
                  and handles it accordingly
     PARAMETERS: 
        client_socket (IN) Socket used to commuticate 
                           with the peer
    ------------------------------------"""
    def __handlePeer( self, client_socket ):
        host, port = client_socket.getpeername()
        if self.debug:
            print 'Received connection request from %s:%d...' % (host, port)
        peer_connection = PeerConnection( self.my_id, 
                                          self.server_host, 
                                          self.server_port, 
                                          client_socket)
        
        try:
            msg_type, msg_data = peer_connection.recvData()
            if msg_type:
                msg_type = msg_type.upper()
            if self.debug:
                print "Received: {%s, %s}" % (msg_type, msg_data)

            if msg_type not in self.handlers:
                if self.debug:
                    print 'Message can not be handled'
            else:
                handler = self.handlers[msg_type]
                handler( peer_connection, 
                         msg_data)
            peer_connection.close()
            
        except KeyboardInterrupt:
            raise

        except:
            traceback.print_exc()


    """------------------------------------
     NAME: __buildPeersTable
     DESCRIPTION: 
     PARAMETERS:
    ------------------------------------"""
    def __buildPeersTable( self, debug=False ):
        if debug:
            my_file = open(self.peer_file, 'r')
            for line in my_file:
                peer_data = line.split(" ")
                size_peer_data = len(peer_data)
                if size_peer_data == 3:
                    self.peer_list.append([peer_data[0], peer_data[1], int(peer_data[2])])
            if self.debug:
                print "This is my peer list: ", self.peer_list

        

    """------------------------------------
     NAME: connectAndSend
     DESCRIPTION: 
     PARAMETERS:
    ------------------------------------"""
    def connectAndSend(self, my_id, remote_host, remote_port, msg_type, msg_data, wait_reply=False):
        msg_reply = []
        try:
            if self.debug:
                print '%s is trying to connect to %s:%d'%(my_id, remote_host, int(remote_port))
            peer_connection = PeerConnection(my_id, remote_host, remote_port)
            rc = peer_connection.sendData(msg_type, msg_data)
            if not rc:
                print 'Error sending data...'
                return msg_reply

            if self.debug:
                print 'Succesfully send data...'
            if wait_reply:
                if self.debug:
                    print 'Waiting for reply...'
                reply = peer_connection.recvData()
                while( reply != (None, None)):
                    msg_reply.append( reply )
                    reply = peer_connection.recvData();
                    
            if self.debug:
                print 'Received: %s' % msg_reply

            peer_connection.close()
            return msg_reply

        except KeyboardInterrupt:
            raise
            return msg_reply

        except:
            traceback.print_exc()
            return msg_reply


    """------------------------------------
     NAME: sendToPeer
     DESCRIPTION: 
     PARAMETERS:
    ------------------------------------"""
    def sendToPeer(self, peer_id, msg_type, msg_data, wait_reply=False):
        (host, port) = self.getPeer(peer_id)
        if host:
            if self.debug:
                print "Sending: {%s, %s}" % (msg_type, msg_data)
            return self.connectAndSend(peer_id,
                                       host, 
                                       port, 
                                       msg_type, 
                                       msg_data, 
                                       wait_reply)
        else:
            print 'Peer does not exist'
            return None

        
    """------------------------------------
     NAME: connectionHandler
     DESCRIPTION: Main loop for receiving connection
                  requests.
     PARAMETERS:
    ------------------------------------"""
    def connectionHandler( self ):
        print 'Starting connection handler...'
        s = self.__makeServerSocket( self.server_port )
        s.settimeout(2)

        while not self.shutdown:
            try:
                client_socket, client_address = s.accept()
                client_socket.settimeout(None)
                t = threading.Thread( target = self.__handlePeer,
                                      args = [ client_socket ] )
                t.start()

            except KeyboardInterrupt:
                print "\nKeyboard interruption. Stopping..."
                self.shutdown = True

            except:
                #traceback.print_exc()
                #self.shutdown = True
                continue

        s.close()

 
    def insertPeer( self, peer_id, host, port ):
        if peer_id not in self.peer_list:
            if self.debug:
                print 'Inserting %s: (%s,%d)'% (peer_id, host, int(port))
            self.peer_list[ peer_id ] = (host, int(port))
            return True
        else:
            return False

    def getPeer( self, peer_id ):
        assert peer_id in self.peer_list
        return self.peer_list[peer_id]


    def deletePeer( self, peer_id ):
        if peer_id in self.peer_list:
            del self.peer_list[peer_id]

    def getPeerIds( self ):
        return self.peer_list.keys()

    def numberOfPeers( self ):
        return len(self.peer_list)

    def maxPeersReached( self ):
        assert self.max_peers == 0 or len(self.peer_list) <= self.max_peers
	return self.max_peers > 0 and len(self.peer_list) == self.max_peers
