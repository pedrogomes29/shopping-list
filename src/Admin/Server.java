package Admin;

import NioChannels.Message.Message;
import NioChannels.Socket.Socket;

import java.io.IOException;

public class Server extends Node.Server {

    public Server(String confFilePath, int port, int nrReplicas, int nrVirtualNodesPerNode) throws IOException {
        super(confFilePath, port, nrReplicas, nrVirtualNodesPerNode, new MessageProcessorBuilder());
        gossiper.addNeighborID(this.nodeId);
        gossiper.addRumour("ADD_ADMIN " + nodeId + " " + port);
        Socket socketToDnsMockup = connectWithoutAddingNeighbor("127.0.0.1", port);
        synchronized (outboundMessageQueue){
            outboundMessageQueue.add(new Message("ADD_ADMIN " +  port, socketToDnsMockup));
        }
    }
}
