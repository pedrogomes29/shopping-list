package RingNode;

import java.io.IOException;
import java.sql.SQLException;

import Database.Database;
import Node.ConsistentHashing.TokenNode;
import RingNode.Synchronizer.Synchronizer;
import Utils.Hasher;

public class Server extends Node.Server
{

    private final Database db;
    public final Synchronizer synchronizer;

    public Thread synchronizerThread;

    public Server(String confFilePath, int nodePort, int nrReplicas, int nrVirtualNodesPerNode) throws IOException, SQLException {
        super(confFilePath, nodePort, nrReplicas, nrVirtualNodesPerNode, new MessageProcessorBuilder());

        TokenNode self = new TokenNode(null,nodeId,null);
        consistentHashing.addNodeToRing(self);
        gossiper.addNeighbor(self);
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

    @Override
    public void close() throws Exception {
        super.close();
        db.close();
    }
}
