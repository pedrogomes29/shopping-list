package LoadBalencer;

import NIOChannels.Message;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

public class MessageProcessor extends NIOChannels.MessageProcessor {
    public MessageProcessor(NIOChannels.Server server, Message message) {
        super(server, message);
    }

    @Override
    public void run() {
        String messageContent = new String(message.bytes);
        if(messageContent.startsWith("ADD_NODE")) {
            String nodeIdStr = messageContent.split(" ")[1];
            TokenNode tokenNode = new TokenNode(message.getSocket(),nodeIdStr,server.getWriteQueue());
            Server loadBalancerServer = (Server) this.server;

            try {
                loadBalancerServer.addNodeToRing(tokenNode);

            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }

        }
    }
}
