#!/usr/bin/python

import socket
import struct


class PeerConnection:

    #-------- CONSTRUCTOR METHODS -----------
    def __init__(self, my_id, remote_host, remote_port, client_socket=None):
        self.my_id = my_id
        if client_socket:
            self.s = client_socket
        else:
            self.s = socket.socket( socket.AF_INET, socket.SOCK_STREAM )
            self.s.connect( (remote_host, int(remote_port)) )
        self.s_descriptor = self.s.makefile('rw', 0)
        self.debug = False



    #-------- PRIVATE METHODS -----------

    #------------------------------------
    # NAME: __marshallMsg
    # DESCRIPTION: Marshalls a message
    # PARAMETERS:
    #   msg_type (IN) type of message according to the protocol
    #   msg_data (IN) data to send
    #------------------------------------
    def __marshallMsg( self, msg_type, msg_data ):
        msg_len = len( msg_data )
        msg = struct.pack( "!4sL%ds" % msg_len, msg_type, msg_len, msg_data )
        return msg



    #-------- PUBLIC METHODS -----------

    #------------------------------------
    # NAME: sendData
    # DESCRIPTION: sends a message of a specific type
    # PARAMETERS: 
    #    msg_type (IN) type of message
    #    msg_data (IN) actual data
    #------------------------------------
    def sendData( self, msg_type, msg_data ):
        msg = self.__marshallMsg( msg_type, msg_data )
        if self.debug:
            print "Marshalled message: {%s}" % (msg)
        self.s_descriptor.write(msg)
        self.s_descriptor.flush()
        return True

    #------------------------------------
    # NAME: recvData
    # DESCRIPTION: receives a message
    # PARAMETERS: None
    #------------------------------------
    def recvData( self ):
        # read the message type
        msg_type = self.s_descriptor.read( 4 )
        if not msg_type:
            return (None, None)

        # read the message length
        string_len = self.s_descriptor.read( 4 )
        msg_len = int( struct.unpack("!L", string_len)[0])
        msg = ""
        
        # keep reading till we complete the message
        while len(msg) != msg_len:
            data = self.s_descriptor.read(min(2048, msg_len-len(msg)))
            if not len(data):
                break
            msg += data

        # make sure we read msg_len bytes
        if len(msg) != msg_len:
            return(None, None)

        return( msg_type, msg )

    #------------------------------------
    # NAME: close
    # DESCRIPTION: cleans up the connection
    # PARAMETERS: None
    #------------------------------------
    def close( self ):
        self.s.close()
        self.s = None
        self.s_descriptor = None
