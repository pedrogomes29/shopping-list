package NIOChannels;

import LoadBalancer.TokenNode;

import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Queue;

public abstract class MessageProcessor implements Runnable {
    protected Server server;
    protected Message message;

    enum ALREADY_SEEN_RUMOUR {
        FALSE,
        TRUE
    }
    public MessageProcessor(Server server, Message message){
        this.server = server;
        this.message = message;
    }

    public void receiveNewNodeWithEndpoint(String newNodeMessage){
        String[] newNodeMessageParts = newNodeMessage.split(" ");

        String newNodeID = newNodeMessageParts[1];

        String newNodeEndpoint = newNodeMessageParts[2];
        String[] newNodeEndpointParts = newNodeEndpoint.split(":");
        String newNodeHost = newNodeEndpointParts[0];
        int newNodePort = Integer.parseInt(newNodeEndpointParts[1]);
        Socket socketToNewNode = server.connect(new InetSocketAddress(newNodeHost,newNodePort));

        String messageContentToNewNode = "NEW_NODE" + " " + server.nodeId;
        try {
            server.addNodeToRing(new TokenNode(socketToNewNode,newNodeID));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        Queue<Message> messageQueue = this.server.getWriteQueue();

        synchronized (messageQueue){
            messageQueue.add(new Message(messageContentToNewNode,socketToNewNode));
        }
    }

    public void receiveNewNode(String newNodeMessage){
        String newNodeID = newNodeMessage.split(" ")[1];
        try {
            server.addNodeToRing(new TokenNode(message.getSocket(),newNodeID));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }


    public void receiveRumour(String rumour){
        Socket rumourSender = message.getSocket();
        Queue<Message> messageQueue = this.server.getWriteQueue();

        if(rumour.startsWith("NEW_NODE ")){
            receiveNewNodeWithEndpoint(rumour);
        }

        String rumourACK;
        if(server.rumours.containsKey(rumour)){
            rumourACK = "RUMOUR_ACK" + " " + ALREADY_SEEN_RUMOUR.TRUE + " " + rumour;
        }
        else{
            rumourACK = "RUMOUR_ACK" + " " + ALREADY_SEEN_RUMOUR.FALSE + " " + rumour;
        }

        synchronized (messageQueue){
            messageQueue.add(new Message(rumourACK,rumourSender));
        }
    }

    public void receiveRumourACK(String rumourACK){
        String[] rumourACKParts = rumourACK.split(" ",2);
        boolean alreadySeenRumour = Boolean.parseBoolean(rumourACKParts[0]);
        if(alreadySeenRumour){
            String rumour =  rumourACKParts[1];
            int rumourCount = server.rumours.get(rumour);
            rumourCount--;
            if(rumourCount<=0)
                server.rumours.remove(rumour);
            else
                server.rumours.put(rumour,rumourCount);
        }
    }

    @Override
    public void run() {
        String messageContent = new String(message.bytes);
        if(messageContent.startsWith("RUMOUR ")) {
            String[] messageParts = messageContent.split(" ", 2);
            String rumour = messageParts[1];
            receiveRumour(rumour);
        }
        else if(messageContent.startsWith("RUMOUR_ACK ")) {
            String[] messageParts = messageContent.split(" ", 2);
            String rumourACK = messageParts[1];
            receiveRumourACK(rumourACK);
        }
        else if(messageContent.startsWith("NEW_NODE ")) {
            receiveNewNode(messageContent);
        }
    }
}
