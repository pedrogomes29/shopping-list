package NioChannels.Message;

import NioChannels.Server;

public abstract class MessageProcessorBuilder {
    protected Server server;

    public void setServer(Server server){
        this.server = server;
    }

    public abstract MessageProcessor build(Message message);

}
