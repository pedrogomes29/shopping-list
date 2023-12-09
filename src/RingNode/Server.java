package RingNode;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Map;

import Database.Database;
import NioChannels.Message.Message;
import NioChannels.Socket.Socket;
import Node.ConsistentHashing.TokenNode;
import RingNode.ConsistentHashing.ConsistentHashing;
import RingNode.Synchronizer.Synchronizer;
import Utils.Hasher;

public class Server extends Node.Server
{

    private final Database db;
    public final Synchronizer synchronizer;

    public Thread synchronizerThread;

    public Server(String nodeId, String confFilePath, int nodePort, int nrReplicas, int nrVirtualNodesPerNode) throws IOException, SQLException {
        super(nodeId, confFilePath, nodePort, nrReplicas, nrVirtualNodesPerNode, new MessageProcessorBuilder());
        this.consistentHashing = new ConsistentHashing(nrReplicas,nrVirtualNodesPerNode,this);

        TokenNode self = new TokenNode(null,nodeId,null);
        ((ConsistentHashing)consistentHashing).addSelfToRing(self);
        gossiper.addRumour("ADD_NODE" + " " + nodeId + " " + port );

        try {
            db = new Database("database/" + nodeId + ".db");
        } catch (SQLException e){
            throw new SQLException("Conecting to database - " + e.getMessage());
        }

        synchronizer = new Synchronizer(this,super.getWriteQueue(),nrReplicas,nrVirtualNodesPerNode);
    }

    public Database getDB(){
        return db;
    }

    public void startThreads()
    {
        super.startThreads();
        synchronizerThread = new Thread(synchronizer);
        synchronizerThread.start();

    }
}
