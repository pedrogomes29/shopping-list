package LoadBalancer;

import Node.Message.Message;
import Node.Socket.Socket;

import java.security.NoSuchAlgorithmException;


public class MessageProcessor extends Node.Message.MessageProcessor {
    public MessageProcessor(Node.Server server, Message message) {
        super(server, message);
    }

    /**
     * Determines whether the received message is a PUT or GET request and identifies the node for
     * redirection (using consistent hasing).
     *
     * @param message The received message.
     * @return The socket to which the message should be redirected.
     */
    @Override
    protected Socket isToRedirectMessage(Message message) {
        String messageContent = new String(message.bytes);
        if(messageContent.startsWith("PUT ") || messageContent.startsWith("GET ")){
            try {
                return server.propagateRequestToNode(message);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }
}
