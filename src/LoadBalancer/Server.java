package LoadBalancer;

import java.io.IOException;

public class Server extends Node.Server
{
    public Server( String confFilePath, int port, int nrReplicas ) throws IOException {
        super(confFilePath, port, nrReplicas, new MessageProcessorBuilder());
        gossiper.addNeighborID(this.nodeId);
        gossiper.addRumour("ADD_LB" + " " + nodeId + " " + port );
    }

}
