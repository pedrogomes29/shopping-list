package LoadBalancer;

import NioChannels.Message.Message;
import NioChannels.Socket.Socket;
import java.io.IOException;
import java.net.InetSocketAddress;

public class Server extends Node.Server
{
    public Server( String confFilePath, int port, int nrReplicas, int nrVirtualNodesPerNode ) throws IOException {
        super(confFilePath, port, nrReplicas, nrVirtualNodesPerNode, new MessageProcessorBuilder());
        gossiper.addNeighbor(new Node.Node(null,this.nodeId,null));
        gossiper.addRumour("ADD_LB" + " " + nodeId + " " + port );
        Socket socketToDnsMockup = connect("127.0.0.1",9090);
        synchronized (outboundMessageQueue){
            outboundMessageQueue.add(new Message("ADD_LB" + " " +  port ,socketToDnsMockup));
        }
    }

}
