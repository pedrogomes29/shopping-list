package LoadBalancer;

import NIOChannels.Message;

import java.security.NoSuchAlgorithmException;


public class MessageProcessor extends NIOChannels.MessageProcessor {
    public MessageProcessor(NIOChannels.Server server, Message message) {
        super(server, message);
    }

    private void addNode(String messageContent){
        String nodeIdStr = messageContent.split(" ")[1];
        TokenNode tokenNode = new TokenNode(message.getSocket(),nodeIdStr);
        Server loadBalancerServer = (Server) this.server;

        try {
            loadBalancerServer.addNodeToRing(tokenNode);

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void run() {
        String messageContent = new String(message.bytes);
        if(messageContent.startsWith("ADD_NODE ")) {
            addNode(messageContent);
        }
        else if(messageContent.startsWith("PUT ") || messageContent.startsWith("GET ")){
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
