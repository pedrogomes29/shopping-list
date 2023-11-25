package LoadBalancer;

import java.io.IOException;

public class Server extends Node.Server
{
    public Server( int port, int nrReplicas ) throws IOException {
        super(port, nrReplicas, new MessageProcessorBuilder());
    }
}
