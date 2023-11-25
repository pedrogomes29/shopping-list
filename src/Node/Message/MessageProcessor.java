package Node.Message;

import LoadBalancer.TokenNode;
import Node.Gossiper.ALREADY_SEEN_RUMOUR;
import Node.Server;
import Node.Socket.Socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Queue;

public abstract class MessageProcessor implements Runnable {
    protected Server server;
    protected Message message;


    public MessageProcessor(Server server, Message message){
        this.server = server;
        this.message = message;
    }

    public String receiveNewNodeWithEndpoint(String newNodeMessage) throws IOException {
        String[] newNodeMessageParts = newNodeMessage.split(" ");

        String method = newNodeMessageParts[0];
        String newNodeID = newNodeMessageParts[1];

        String newNodeEndpoint = newNodeMessageParts[2];
        String[] newNodeEndpointParts = newNodeEndpoint.split(":");
        String newNodeHost;
        boolean firstOneConnected = false;
        int newNodePort;
        if (newNodeEndpointParts.length > 1) {
            newNodeHost = newNodeEndpointParts[0];
            newNodePort = Integer.parseInt(newNodeEndpointParts[1]);
        }else{
            InetSocketAddress address = (InetSocketAddress) this.message.getSocket().socketChannel.getRemoteAddress();
            newNodeHost = address.getHostString();
            newNodePort = Integer.parseInt(newNodeEndpointParts[0]);
            firstOneConnected = true;
        }

        try {
            if (!server.containsNode(newNodeID)){
                Socket socketToNewNode;
                if (firstOneConnected)
                    socketToNewNode = this.message.getSocket();
                else
                    socketToNewNode = server.connect(newNodeHost, newNodePort);

                server.addNodeToRing(new TokenNode(socketToNewNode,newNodeID));

                String messageContentToNewNode = "ADD_NODE" + " " + server.getNodeId();

                Queue<Message> messageQueue = this.server.getWriteQueue();

                synchronized (messageQueue){
                    messageQueue.add(new Message(messageContentToNewNode,socketToNewNode));
                }
            }

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        return method + " " + newNodeID + " " + newNodeHost + ":" + newNodePort;

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
        boolean alreadyReceivedRumour = false;
        String newrumour = rumour;
        if(rumour.startsWith("ADD_NODE ")){
            try {
                String nodeID = rumour.split(" ")[1];
                alreadyReceivedRumour = server.containsNode(nodeID);

                newrumour = receiveNewNodeWithEndpoint(rumour);


            } catch (IOException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        String rumourACK;
        if(server.getRumours().containsKey(rumour) || alreadyReceivedRumour){
            rumourACK = "RUMOUR_ACK" + " " + ALREADY_SEEN_RUMOUR.TRUE + " " + rumour;
        }
        else{
            server.addRumour(newrumour);
            rumourACK = "RUMOUR_ACK" + " " + ALREADY_SEEN_RUMOUR.FALSE + " " + rumour;
        }

        synchronized (messageQueue){
            messageQueue.add(new Message(rumourACK,rumourSender));
        }

    }

    public void receiveRumourACK(String rumourACK){
        String[] rumourACKParts = rumourACK.split(" ",2);
        boolean alreadySeenRumour =rumourACKParts[0].equals("1");
        if(alreadySeenRumour){
            String rumour =  rumourACKParts[1];
            int rumourCount = server.getRumours().getOrDefault(rumour, -1);
            rumourCount--;
            if(rumourCount<=0)
                server.getRumours().remove(rumour);
            else
                server.getRumours().put(rumour,rumourCount);
        }
    }

    @Override
    public void run() {
        String messageContent = new String(message.bytes);

        if(messageContent.startsWith("RUMOUR ")) {
            // first one send
            String[] messageParts = messageContent.split(" ", 2);
            String rumour = messageParts[1];
            receiveRumour(rumour);
        }
        else if(messageContent.startsWith("RUMOUR_ACK ")) {
            String[] messageParts = messageContent.split(" ", 2);
            String rumourACK = messageParts[1];
            receiveRumourACK(rumourACK);
        }
        else if(messageContent.startsWith("ADD_NODE ")) {
            receiveNewNode(messageContent);
        }
    }
}
