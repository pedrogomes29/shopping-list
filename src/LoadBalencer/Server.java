package LoadBalencer;

public class Server extends NIOChannels.Server
{
    public Server( int port )
    {
        super(port, new MessageProcessorBuilder());

    }

}
