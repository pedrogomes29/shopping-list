package Admin;

import NioChannels.Message.Message;
import NioChannels.Socket.Socket;

public class MessageProcessor extends Node.Message.MessageProcessor {

    public MessageProcessor(Node.Server server, Message message) {
        super(server, message);
    }

    @Override
    protected Socket isToRedirectMessage(Message message) {
        return null;
    }
}
