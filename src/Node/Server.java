package Node;

import LoadBalancer.TokenNode;
import NIOChannels.Message;
import NIOChannels.Socket;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.sql.SQLException;
import java.util.*;

import Database.Database;

public class Server extends NIOChannels.Server
{

    private ArrayList<String> nodeHashes;
    private HashMap<String, TokenNode> hashToNode;
    private Database db;

    public Server( int nodePort )
    {
        super(nodePort, new MessageProcessorBuilder());
        nodeHashes = new ArrayList<>();
        hashToNode = new HashMap<>();
        try {
            db = new Database("database/"+nodeId+".db");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void startGossip(Socket socketToGossipTo){
        Message gossipMessage = new Message("ADD_NODE " + nodeId,socketToGossipTo);
        synchronized (outboundMessageQueue){
            outboundMessageQueue.add(gossipMessage);
        }
    }





    public Database getDB(){
        return db;
    }


}
