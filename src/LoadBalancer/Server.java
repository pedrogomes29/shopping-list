package LoadBalancer;

import NioChannels.Message.Message;
import NioChannels.Socket.Socket;
import java.io.IOException;

public class Server extends Node.Server
{
    public Server( String confFilePath, int port, int nrReplicas, int nrVirtualNodesPerNode ) throws IOException {
        super(confFilePath, port, nrReplicas, nrVirtualNodesPerNode, new MessageProcessorBuilder());
        gossiper.addNeighborID(this.nodeId);
        gossiper.addRumour("ADD_LB" + " " + nodeId + " " + port );
        Socket socketToDnsMockup = connectWithoutAddingNeighbor("127.0.0.1",9090);
        synchronized (outboundMessageQueue){
            outboundMessageQueue.add(new Message("ADD_LB" + " " +  port ,socketToDnsMockup));
        }
    }

}
