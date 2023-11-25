package LoadBalancer;

import Node.Message.Message;

import java.security.NoSuchAlgorithmException;


public class MessageProcessor extends Node.Message.MessageProcessor {
    public MessageProcessor(Node.Server server, Message message) {
        super(server, message);
    }

    @Override
    public void run() {
        super.run();
        String messageContent = new String(message.bytes);
        if(messageContent.startsWith("PUT ") || messageContent.startsWith("GET ")){
            try {
                ((Server)server).propagateRequestToNode(message);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
        else if(messageContent.startsWith("PUT_ACK ") || messageContent.startsWith("GET_RESPONSE "))
            ((Server)server).propagateResponseToClient(message.bytes);
    }
}
