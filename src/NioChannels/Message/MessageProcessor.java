package NioChannels.Message;

import NioChannels.Server;

public abstract class MessageProcessor implements Runnable {
    protected Server server;
    protected Message message;


    public MessageProcessor(Server server, Message message){
        this.server = server;
        this.message = message;
    }

    @Override
    public abstract void run();

}
