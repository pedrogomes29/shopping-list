package Node.Message;

import NioChannels.Message.Message;
import Node.ConsistentHashing.TokenNode;
import Node.Gossiper.ALREADY_SEEN_RUMOUR;
import Node.Server;
import NioChannels.Socket.Socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Objects;
import java.util.Queue;

public abstract class MessageProcessor extends NioChannels.Message.MessageProcessor {

    public MessageProcessor(Server server, Message message){
        super(server, message);
    }

    public void sendMessageToNewNode(Socket socketToNewNode){
        String messageContentToNewNode = "";
        System.out.println(this.getClass());
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

        if(Objects.equals(method, "ADD_NODE")){
            if (!getServer().knowsAboutRingNode(newNodeID)){
                Socket socketToNewNode = getNewNodeSocket(newNodeHost,newNodePort,firstOneConnected);
                TokenNode tokenNode = new TokenNode(socketToNewNode,newNodeID);
                if (getServer().consistentHashing.addNodeToRing(tokenNode))
                    getServer().gossiper.addNeighbor(tokenNode.getSocket());

                sendMessageToNewNode(socketToNewNode);
            }

        }
        else if(Objects.equals(method, "ADD_LB")){
            if (!getServer().knowsAboutLBNode(newNodeID)){
                Socket socketToNewNode = getNewNodeSocket(newNodeHost,newNodePort,firstOneConnected);
                getServer().addLBNode(socketToNewNode,newNodeID);
                sendMessageToNewNode(socketToNewNode);
            }
        }

        return method + " " + newNodeID + " " + newNodeHost + ":" + newNodePort;

    }

    public void receiveNewNode(String newNodeMessage){
        String newNodeID = newNodeMessage.split(" ")[1];

        TokenNode tokenNode = new TokenNode(message.getSocket(),newNodeID);
        if (getServer().consistentHashing.addNodeToRing(tokenNode))
            getServer().gossiper.addNeighbor(tokenNode.getSocket());

    }

    public void receiveNewLB(String newNodeMessage){
        String newNodeID = newNodeMessage.split(" ")[1];
        getServer().addLBNode(message.getSocket(),newNodeID);
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

            } catch (IOException e) {
                System.err.println("Error: received rumour, but cant get address of the sender");
                return;
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

    /**
     * Process incoming messages, identifies the message type,
     * and invokes the appropriate handler methods based on the message type.
     */
    @Override
    public void run() {
        String messageContent = new String(message.bytes);

        String[] messageParts = messageContent.split(" ", 2);
        String messageType = messageParts[0];

        switch (messageType) {
            case "RUMOUR":
                receiveRumour(messageParts[1]);
                break;
            case "RUMOUR_ACK":
                receiveRumourACK(messageParts[1]);
                break;
            case "ADD_NODE":
                receiveNewNode(messageContent);
                break;
            case "ADD_LB":
                receiveNewLB(messageContent);
                break;
            case "PUT":
            case "GET":
                receivePutGet(message);
                break;
            case "GET_NACK":
            case "PUT_ACK":
                receiveReply(messageContent, 2);
                break;
            case "PUT_NACK":
            case "GET_RESPONSE":
                receiveReply(messageContent, 3);
                break;
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

    protected void closeSocket(Socket socket){


    }
}
