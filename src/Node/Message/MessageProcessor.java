package Node.Message;

import LoadBalancer.TokenNode;
import Node.Gossiper.ALREADY_SEEN_RUMOUR;
import Node.Server;
import Node.Socket.Socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Queue;

public abstract class MessageProcessor implements Runnable {
    protected Server server;
    protected Message message;


    public MessageProcessor(Server server, Message message){
        this.server = server;
        this.message = message;
    }

    public void sendMessageToNewNode(Socket socketToNewNode){
        String messageContentToNewNode = "";
        System.out.println(this.getClass());
        if (this instanceof LoadBalancer.MessageProcessor)
            messageContentToNewNode = "ADD_LB" + " " + server.getNodeId();
        else if(this instanceof RingNode.MessageProcessor)
            messageContentToNewNode = "ADD_NODE" + " " + server.getNodeId();

        if(!messageContentToNewNode.isEmpty()) {
            Queue<Message> messageQueue = this.server.getWriteQueue();

            synchronized (messageQueue) {
                messageQueue.add(new Message(messageContentToNewNode, socketToNewNode));
            }
        }

    }

    private Socket getNewNodeSocket(String host, int port, boolean firstOneConnected){
        if (firstOneConnected)
            return this.message.getSocket();
        else
            return server.connect(host, port);
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
            if(Objects.equals(method, "ADD_NODE")){
                if (!server.knowsAboutRingNode(newNodeID)){
                    Socket socketToNewNode = getNewNodeSocket(newNodeHost,newNodePort,firstOneConnected);
                    server.addNodeToRing(new TokenNode(socketToNewNode,newNodeID));
                    sendMessageToNewNode(socketToNewNode);
                }


            }
            else if(Objects.equals(method, "ADD_LB")){
                if (!server.knowsAboutLBNode(newNodeID)){
                    Socket socketToNewNode = getNewNodeSocket(newNodeHost,newNodePort,firstOneConnected);
                    server.addLBNode(socketToNewNode,newNodeID);
                    sendMessageToNewNode(socketToNewNode);
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

    public void receiveNewLB(String newNodeMessage){
        String newNodeID = newNodeMessage.split(" ")[1];
        server.addLBNode(message.getSocket(),newNodeID);
    }



    public void receiveRumour(String rumour){
        Socket rumourSender = message.getSocket();
        Queue<Message> messageQueue = this.server.getWriteQueue();
        boolean alreadyReceivedRumour = false;
        String newrumour = rumour;
        boolean addRingNodeRumour = rumour.startsWith("ADD_NODE ");
        boolean addLBNodeRumour = rumour.startsWith("ADD_LB ");

        if(addRingNodeRumour || addLBNodeRumour){
            try {
                String nodeID = rumour.split(" ")[1];

                if(addRingNodeRumour)
                    alreadyReceivedRumour = server.knowsAboutRingNode(nodeID);
                else
                    alreadyReceivedRumour = server.knowsAboutLBNode(nodeID);


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

        else if(messageContent.startsWith("ADD_LB ")) {
            receiveNewLB(messageContent);
        }
    }
}
