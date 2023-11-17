package NIOChannels;

public abstract class MessageProcessorBuilder {
    protected  Server server;


    protected void setServer(Server server){
        this.server = server;
    }

    public abstract MessageProcessor build(Message message);

}
