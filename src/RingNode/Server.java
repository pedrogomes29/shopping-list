package RingNode;

import java.io.IOException;
import java.sql.SQLException;

import Database.Database;
import Node.ConsistentHashing.TokenNode;

public class Server extends Node.Server
{

    private final Database db;

    public Server(String confFilePath, int nodePort, int nrReplicas, int nrVirtualNodesPerNode) throws IOException, SQLException {
        super(confFilePath, nodePort, nrReplicas, nrVirtualNodesPerNode, new MessageProcessorBuilder());

        TokenNode self = new TokenNode(null,nodeId);
        consistentHashing.addNodeToRing(self);
        gossiper.addRumour("ADD_NODE" + " " + nodeId + " " + port );

        try {
            db = new Database("database/" + nodeId + ".db");
        } catch (SQLException e){
            throw new SQLException("Conecting to database - " + e.getMessage());
        }
    }

    public Database getDB(){
        return db;
    }

    @Override
    public void close() throws Exception {
        super.close();
        db.close();
    }
}
