package Admin;

import NioChannels.Message.Message;
import NioChannels.Socket.Socket;
import Node.ConsistentHashing.ConsistentHashing;

import java.io.IOException;

public class Server extends Node.Server {

    public Server(String confFilePath, int port, int nrReplicas, int nrVirtualNodesPerNode) throws IOException {
        super(confFilePath, port, nrReplicas, nrVirtualNodesPerNode, new MessageProcessorBuilder());
        this.consistentHashing = new ConsistentHashing(nrReplicas, nrVirtualNodesPerNode);

        gossiper.addRumour("ADD_ADMIN " + nodeId + " " + port);
        Socket socketToDnsMockup = connect("127.0.0.1",9090);
        synchronized (outboundMessageQueue){
            outboundMessageQueue.add(new Message("ADD_ADMIN " +  port, socketToDnsMockup));
        }
    }
}
