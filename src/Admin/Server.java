package Admin;

import LoadBalancer.MessageProcessorBuilder;
import Node.ConsistentHashing.ConsistentHashing;

import java.io.IOException;

public class Server extends Node.Server {

    public Server(String nodeId, String confFilePath, int port, int nrReplicas, int nrVirtualNodesPerNode) throws IOException {
        super(nodeId, confFilePath, port, nrReplicas, nrVirtualNodesPerNode, new MessageProcessorBuilder());
        this.consistentHashing = new ConsistentHashing(nrReplicas, nrVirtualNodesPerNode);

        gossiper.addRumour("ADD_ADMIN " + nodeId + " " + port);
    }
}
