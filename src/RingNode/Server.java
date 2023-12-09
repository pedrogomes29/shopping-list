package RingNode;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import Database.Database;
import Node.ConsistentHashing.TokenNode;
import RingNode.ConsistentHashing.ConsistentHashing;
import RingNode.Synchronizer.Synchronizer;

public class Server extends Node.Server
{

    private final Database db;
    public final Synchronizer synchronizer;

    public Thread synchronizerThread;

    public Server(String nodeId, String confFilePath, int nodePort, int nrReplicas, int nrVirtualNodesPerNode) throws IOException {
        super(nodeId, confFilePath, nodePort, nrReplicas, nrVirtualNodesPerNode, new MessageProcessorBuilder());
        this.consistentHashing = new ConsistentHashing(nrReplicas,nrVirtualNodesPerNode,this);

        try {
            TokenNode self = new TokenNode(null,nodeId,null);
            ((ConsistentHashing)consistentHashing).addSelfToRing(self);
            gossiper.addRumour("ADD_NODE" + " " + nodeId + " " + port );
            db = new Database("database/"+nodeId+".db");
            synchronizer = new Synchronizer(this,super.getWriteQueue(),nrReplicas,nrVirtualNodesPerNode);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public StringBuilder sendShoppingListsToNeighbor(String startingHash, String endingHash){
        Map<String, String> shoppingLists = getDB().getShoppingListsBase64(startingHash, endingHash);
        StringBuilder synchronizationMessageBuilder = new StringBuilder("SYNCHRONIZE_RESPONSE" + " ");

        for (String shoppingListID : shoppingLists.keySet()) {
            String shoppingListBase64 = shoppingLists.get(shoppingListID);
            synchronizationMessageBuilder.append(shoppingListID).append(":").append(shoppingListBase64).append(',');
        }

        return synchronizationMessageBuilder;

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
