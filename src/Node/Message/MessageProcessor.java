package Node.Message;

import NioChannels.Message.Message;
import Node.ConsistentHashing.TokenNode;
import Node.Gossiper.ALREADY_SEEN_RUMOUR;
import Node.Server;
import NioChannels.Socket.Socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Queue;
import Node.Node;

public abstract class MessageProcessor extends NioChannels.Message.MessageProcessor {

    public MessageProcessor(Server server, Message message){
        super(server, message);
    }

    public void sendMessageToNewNode(Socket socketToNewNode){
        String messageContentToNewNode = "";
        if (this instanceof LoadBalancer.MessageProcessor)
            messageContentToNewNode = "ADD_LB" + " " + getServer().getNodeId();
        else if(this instanceof RingNode.MessageProcessor)
            messageContentToNewNode = "ADD_NODE" + " " + getServer().getNodeId();

        if(!messageContentToNewNode.isEmpty()) {
            Queue<Message> messageQueue = getServer().getWriteQueue();

            synchronized (messageQueue) {
                messageQueue.add(new Message(messageContentToNewNode, socketToNewNode));
            }
        }

    }

    protected Server getServer(){
        return  (Server) server;
    }

    private Socket getNewNodeSocket(String nodeID, String host, int port, boolean firstOneConnected){
        if (firstOneConnected)
            return this.message.getSocket();
        else
            return getServer().connect(nodeID, host, port);
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
                if (!getServer().knowsAboutRingNode(newNodeID)){
                    Socket socketToNewNode = getNewNodeSocket(newNodeID,newNodeHost,newNodePort,firstOneConnected);
                    TokenNode tokenNode = new TokenNode(socketToNewNode,newNodeID);
                    if (getServer().consistentHashing.addNodeToRing(tokenNode))
                        getServer().gossiper.addNeighbor(tokenNode);

                    sendMessageToNewNode(socketToNewNode);
                }

            }
            else if(Objects.equals(method, "ADD_LB")){
                if (!getServer().knowsAboutLBNode(newNodeID)){
                    Socket socketToNewNode = getNewNodeSocket(newNodeID,newNodeHost,newNodePort,firstOneConnected);
                    getServer().addLBNode(new Node(socketToNewNode,newNodeID));
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

            TokenNode tokenNode = new TokenNode(message.getSocket(),newNodeID);
            if (getServer().consistentHashing.addNodeToRing(tokenNode))
                getServer().gossiper.addNeighbor(tokenNode);

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public void receiveNewLB(String newNodeMessage){
        String newNodeID = newNodeMessage.split(" ")[1];
        getServer().addLBNode(new Node(message.getSocket(),newNodeID));
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
                    alreadyReceivedRumour = getServer().knowsAboutRingNode(nodeID);
                else
                    alreadyReceivedRumour = getServer().knowsAboutLBNode(nodeID);


                newrumour = receiveNewNodeWithEndpoint(rumour);


            } catch (IOException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        String rumourACK;
        if(getServer().gossiper.getRumours().containsKey(rumour) || alreadyReceivedRumour){
            rumourACK = "RUMOUR_ACK" + " " + ALREADY_SEEN_RUMOUR.TRUE + " " + rumour;
        }
        else{
            getServer().gossiper.addRumour(newrumour);
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
            int rumourCount = getServer().gossiper.getRumours().getOrDefault(rumour, -1);
            rumourCount--;
            if(rumourCount<=0)
                getServer().gossiper.getRumours().remove(rumour);
            else
                getServer().gossiper.getRumours().put(rumour,rumourCount);
        }
    }

    public void receiveNeighborIDs(String messageContent) {
        String[] messageParts = messageContent.split(" ",2);
        if(messageParts.length<=1)
            return;
        String neighborIDs = messageParts[1];
        String[] neighborIDsParts = neighborIDs.split(",");

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
            receiveRumourACK(messageContent);
        }
        else if(messageContent.startsWith("SYNC_TOPOLOGY ")) {
            String[] messageParts = messageContent.split(" ", 2);
            String neighborIDs = messageParts[1];
            receiveNeighborIDs(neighborIDs);
        }
        else if(messageContent.startsWith("ADD_NODE ")) {
            receiveNewNode(messageContent);
        }else if(messageContent.startsWith("ADD_LB ")) {
            receiveNewLB(messageContent);
        }else if(messageContent.startsWith("PUT ") || messageContent.startsWith("GET ")){
            receivePutGet(message);
        }else if(messageContent.startsWith("PUT_ACK")){
            receiveReply(messageContent, 2);
        }else if(messageContent.startsWith("GET_RESPONSE")){
            receiveReply(messageContent, 3);
        }
    }

    /**
     * Receives an ACK message and processes it, considering the specified message format.
     *
     * @param messageContent The content of the received message.
     * @param dataLength The expected number of components in the message (splits with spaces) without redirects.
     *                   For "put" operations, dataLength is 2; for "get" operations, it is 3.
     */
    private void receiveReply(String messageContent, int dataLength) {
        String[] messageParts = messageContent.split(" ");

        if (messageParts.length > dataLength){
            String redirectSocketID = messageParts[messageParts.length-1];
            Socket redirectSocket = server.getSocketMap().get(Long.parseLong(redirectSocketID));

            Queue<Message> messageQueue = this.server.getWriteQueue();

            // redirect the message to the socket
            String[] messagesWithoutRedirect =Arrays.copyOfRange(messageParts, 0, messageParts.length - 1);
            String messageContentWithoutRedirect = String.join(" ", messagesWithoutRedirect);

            Message message = new Message(messageContentWithoutRedirect, redirectSocket);
            synchronized (messageQueue) {
                messageQueue.offer(message);
            }

        }else {
            throw new RuntimeException("ACK MESSAGE WITHOUT REDIRECT");
        }
    }

    private void receivePutGet(Message message){
        Socket socketToRedirect = isToRedirectMessage(message);
        if (socketToRedirect != null){
            String messageContent = new String(message.bytes);
            this.redirectMessage(messageContent, message.getSocket(), socketToRedirect);
        }
    }

    /**
     * Checks if the received message is a PUT or GET request and determines if redirection is necessary.
     * If redirection is needed, it propagates the request to the appropriate node.
     *
     * @param message The received message.
     * @return The socket to which the message should be redirected, or null if no redirection is required.
     */
    protected abstract Socket isToRedirectMessage(Message message);


    /**
     * Redirects a message to a specified node and appends the original socket ID to the end of the message.
     * This allows the response to travel back to the origin node.
     *
     * @param messageContent The content of the message to be redirected.
     * @param socket The original socket from which the message was received.
     * @param nodeToRedirect The socket of the node to which the message should be redirected.
     */
    private void redirectMessage(String messageContent, Socket socket, Socket nodeToRedirect){
        Queue<Message> messageQueue = this.server.getWriteQueue();

        // redirect message but save the socket id to not lose the connection to send the ack when receive the response
        Message message = new Message(messageContent + " " + socket.getSocketId(), nodeToRedirect);
        synchronized (messageQueue) {
            messageQueue.offer(message);
        }


    }
}
