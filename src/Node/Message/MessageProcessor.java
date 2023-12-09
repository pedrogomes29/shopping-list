package Node.Message;

import NioChannels.Message.Message;
import Node.ConsistentHashing.TokenNode;
import Node.Gossiper.ALREADY_SEEN_RUMOUR;
import Node.Server;
import NioChannels.Socket.Socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import Node.Node;

public abstract class MessageProcessor extends NioChannels.Message.MessageProcessor {

    public MessageProcessor(Server server, Message message){
        super(server, message);
    }

    public void sendMessageToNewNode(Socket socketToNewNode){
        String messageContentToNewNode = "";
        if (this instanceof LoadBalancer.MessageProcessor)
            messageContentToNewNode = "ADD_LB" + " " + getServer().getNodeId() + " " + getServer().port;
        else if (this instanceof Admin.MessageProcessor)
            messageContentToNewNode = "ADD_ADMIN " + getServer().getNodeId() + " " + getServer().port;
        else if (this instanceof RingNode.MessageProcessor)
            messageContentToNewNode = "ADD_NODE" + " " + getServer().getNodeId() + " " + getServer().port;

        if (!messageContentToNewNode.isEmpty()) {
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
            return getServer().connect(host, port);
    }

    public void addNode(String nodeID, String nodeHost, int nodePort, Socket socketToNode) throws NoSuchAlgorithmException {
        InetSocketAddress newNodeEndpointSocketAddress = new InetSocketAddress(nodeHost, nodePort);
        TokenNode tokenNode = new TokenNode(socketToNode,nodeID,newNodeEndpointSocketAddress);
        if (getServer().consistentHashing.addNodeToRing(tokenNode))
            getServer().gossiper.addNeighbor(tokenNode);

        sendMessageToNewNode(socketToNode);
    }

    public void addLB(String nodeID, String nodeHost, int nodePort, Socket socketToNode) {
        InetSocketAddress newNodeEndpointSocketAddress = new InetSocketAddress(nodeHost, nodePort);
        Node lbNode = new Node(socketToNode,nodeID,newNodeEndpointSocketAddress);
        getServer().gossiper.addNeighbor(lbNode);
        sendMessageToNewNode(socketToNode);
    }

    public void addAdmin(String nodeID, String nodeHost, int nodePort, Socket socketToNode) {
        InetSocketAddress newNodeEndpointSocketAddress = new InetSocketAddress(nodeHost, nodePort);
        Node adminNode = new Node(socketToNode, nodeID, newNodeEndpointSocketAddress);
        getServer().gossiper.addNeighbor(adminNode);
        sendMessageToNewNode(socketToNode);
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
        } else {
            InetSocketAddress address = (InetSocketAddress) this.message.getSocket().socketChannel.getRemoteAddress();
            newNodeHost = address.getHostString();
            newNodePort = Integer.parseInt(newNodeEndpointParts[0]);
            firstOneConnected = true;
        }

        try {
            if (Objects.equals(method, "ADD_NODE")) {
                if (!getServer().knowsAboutRingNode(newNodeID)) {
                    Socket socketToNewNode = getNewNodeSocket(newNodeHost, newNodePort, firstOneConnected);
                    addNode(newNodeID, newNodeHost, newNodePort, socketToNewNode);
                }
            }
            else if (Objects.equals(method, "ADD_LB")) {
                if (!getServer().knowsAboutLBNode(newNodeID)) {
                    Socket socketToNewNode = getNewNodeSocket(newNodeHost, newNodePort, firstOneConnected);
                    addLB(newNodeID, newNodeHost, newNodePort, socketToNewNode);
                }
            }
            else if (Objects.equals(method, "ADD_ADMIN")) {
                if (!getServer().knowsAboutAdminNode(newNodeID)) {
                    Socket socketToNewNode = getNewNodeSocket(newNodeHost, newNodePort, firstOneConnected);
                    addAdmin(newNodeID, newNodeHost, newNodePort, socketToNewNode);
                }
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        return method + " " + newNodeID + " " + newNodeHost + ":" + newNodePort;
    }

    public void receiveNewNode(String newNodeMessage) {
        String[] newNodeMessageParts = newNodeMessage.split(" ");
        String newNodeID = newNodeMessageParts[1];
        int newNodePort = Integer.parseInt(newNodeMessageParts[2]);
        try {
            InetSocketAddress address = (InetSocketAddress) this.message.getSocket().socketChannel.getRemoteAddress();
            String newNodeHost = address.getHostString();

            InetSocketAddress newNodeEndpoint = new InetSocketAddress(newNodeHost,newNodePort);
            TokenNode tokenNode = new TokenNode(message.getSocket(),newNodeID, newNodeEndpoint);
            if (getServer().consistentHashing.addNodeToRing(tokenNode))
                getServer().gossiper.addNeighbor(tokenNode);

        } catch (NoSuchAlgorithmException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void receiveNewLB(String newNodeMessage) {
        String[] newLBMessageParts = newNodeMessage.split(" ");
        String newNodeID = newLBMessageParts[1];
        int newNodePort = Integer.parseInt(newLBMessageParts[2]);

        InetSocketAddress address;
        try {
            address = (InetSocketAddress) this.message.getSocket().socketChannel.getRemoteAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String newNodeHost = address.getHostString();
        InetSocketAddress newNodeEndpoint = new InetSocketAddress(newNodeHost,newNodePort);

        Node lbNode = new Node(message.getSocket(),newNodeID, newNodeEndpoint);
        getServer().gossiper.addNeighbor(lbNode);
    }

    public void receiveNewAdmin(String newNodeMessage) {
        String[] newAdminMessageParts = newNodeMessage.split(" ");
        String newNodeID = newAdminMessageParts[1];
        int newNodePort = Integer.parseInt(newAdminMessageParts[2]);

        InetSocketAddress address;
        try {
            address = (InetSocketAddress) this.message.getSocket().socketChannel.getRemoteAddress();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String newNodeHost = address.getHostString();
        InetSocketAddress newNodeEndpoint = new InetSocketAddress(newNodeHost,newNodePort);
        Node adminNode = new Node(message.getSocket(), newNodeID, newNodeEndpoint);
    }

    public String receiveRemoveMessage(String removeNodeMessage) throws NoSuchAlgorithmException {
        String nodeID = removeNodeMessage.split(" ")[1];
        if (!getServer().alreadyRemovedNode(nodeID)) {
            for (TokenNode tokenNode: getServer().consistentHashing.getHashToNode().values()) {
                if (Objects.equals(tokenNode.getId(), nodeID)) {
                    getServer().consistentHashing.removeNodeFromRing(tokenNode);
                }
            }
        }
        return "REMOVE " + nodeID;
    }

    public void receiveRumour(String rumour) throws NoSuchAlgorithmException {
        Socket rumourSender = message.getSocket();
        Queue<Message> messageQueue = this.server.getWriteQueue();
        boolean alreadyReceivedRumour = false;
        String newrumour = rumour;
        boolean addRingNodeRumour = rumour.startsWith("ADD_NODE ");
        boolean addLBNodeRumour = rumour.startsWith("ADD_LB ");
        boolean addAdminNodeRumour = rumour.startsWith("ADD_ADMIN ");
        boolean removeNodeRumour = rumour.startsWith("REMOVE ");

        if (addRingNodeRumour || addLBNodeRumour || addAdminNodeRumour) {
            try {
                String nodeID = rumour.split(" ")[1];
                if (addRingNodeRumour)
                    alreadyReceivedRumour = getServer().knowsAboutRingNode(nodeID);
                else if (addLBNodeRumour)
                    alreadyReceivedRumour = getServer().knowsAboutLBNode(nodeID);
                else
                    alreadyReceivedRumour = getServer().knowsAboutAdminNode(nodeID);

                newrumour = receiveNewNodeWithEndpoint(rumour);
            } catch (IOException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        } else if (removeNodeRumour) {
            String nodeID = rumour.split(" ")[1];
            alreadyReceivedRumour = getServer().alreadyRemovedNode(nodeID);
            newrumour = receiveRemoveMessage(rumour);
        }

        String rumourACK;
        if (getServer().gossiper.getRumours().containsKey(rumour) || alreadyReceivedRumour) {
            rumourACK = "RUMOUR_ACK" + " " + ALREADY_SEEN_RUMOUR.TRUE + " " + rumour;
        }
        else {
            getServer().gossiper.addRumour(newrumour);
            rumourACK = "RUMOUR_ACK" + " " + ALREADY_SEEN_RUMOUR.FALSE + " " + rumour;
        }

        synchronized (messageQueue) {
            messageQueue.add(new Message(rumourACK,rumourSender));
        }
    }

    public void receiveRumourACK(String rumourACK) {
        String[] rumourACKParts = rumourACK.split(" ",2);
        boolean alreadySeenRumour = rumourACKParts[0].equals("1");
        if (alreadySeenRumour) {
            String rumour =  rumourACKParts[1];
            int rumourCount = getServer().gossiper.getRumours().getOrDefault(rumour, -1);
            rumourCount--;
            if (rumourCount<=0)
                getServer().gossiper.getRumours().remove(rumour);
            else
                getServer().gossiper.getRumours().put(rumour,rumourCount);
        }
    }

    public void syncTopology(String messageContent){
        String[] messageParts = messageContent.split(" ",3);
        if (messageParts[2].isEmpty())
            return;

        String nodeToSyncWithID = messageParts[1];
        Map<String,Node> neighbors = getServer().gossiper.getNeighbors();
        Set<String> neighborIDs = new HashSet<>(getServer().gossiper.getNeighbors().keySet());
        neighborIDs.remove(nodeToSyncWithID);

        Queue<Message> writeQueue = this.server.getWriteQueue();

        String topology = messageParts[2];
        String[] topologyParts = topology.split(",");
        for (String node:topologyParts) {
            String[] nodeParts = node.split(" ");
            String nodeID = nodeParts[1];
            if (!neighborIDs.contains(nodeID)) { //unknown node
                String nodeType = nodeParts[0];
                String nodeEndpoint = nodeParts[2];
                String[] nodeEndpointParts = nodeEndpoint.split(":");
                String nodeHost = nodeEndpointParts[0];
                int nodePort = Integer.parseInt(nodeEndpointParts[1]);
                Socket socketToNode = getServer().connect(nodeHost,nodePort);
                if (Objects.equals(nodeType, "NODE")) {
                    try {
                        addNode(nodeID,nodeHost,nodePort,socketToNode);
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException(e);
                    }
                }
                else if (Objects.equals(nodeType, "LB")){
                    addLB(nodeID,nodeHost,nodePort,socketToNode);
                }
            }
            else
                neighborIDs.remove(nodeID);
        }

        //all received neighbor IDs were removed in the loop, only the ones not received are left
        for (String nodeNotReceivedID: neighborIDs) {
            Node nodeNotReceived = neighbors.get(nodeNotReceivedID);
            String messageToAddNode;
            if (nodeNotReceived instanceof TokenNode) {
                System.out.println(nodeNotReceived.getClass());
                messageToAddNode = "REQUEST_ADD_NODE";
            }
            else {
                messageToAddNode = "REQUEST_ADD_LB";
            }
            messageToAddNode += " " + nodeNotReceived.getId() + " " +
                    nodeNotReceived.getNodeEndpoint().getHostName() + ":" + nodeNotReceived.getNodeEndpoint().getPort();
            synchronized (writeQueue){
                writeQueue.add(new Message(messageToAddNode,message.getSocket()));
            }
        }

    }

    public void requestAddNode(String messageContent) {
        String[] messageContentParts = messageContent.split(" ");
        String newNodeID = messageContentParts[1];
        String newNodeEndpoint = messageContentParts[2];
        String[] newNodeEndpointParts = newNodeEndpoint.split(":");
        String newNodeHost = newNodeEndpointParts[0];
        int newNodePort = Integer.parseInt(newNodeEndpointParts[1]);
        Socket newNodeSocket = getServer().connect(newNodeHost,newNodePort);
        try {
            addNode(newNodeID,newNodeHost,newNodePort,newNodeSocket);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public void requestAddLB(String messageContent){
        String[] messageContentParts = messageContent.split(" ");
        String newNodeID = messageContentParts[1];
        String newNodeEndpoint = messageContentParts[2];
        String[] newNodeEndpointParts = newNodeEndpoint.split(":");
        String newNodeHost = newNodeEndpointParts[0];
        int newNodePort = Integer.parseInt(newNodeEndpointParts[1]);
        Socket newNodeSocket = getServer().connect(newNodeHost,newNodePort);
        addLB(newNodeID,newNodeHost,newNodePort,newNodeSocket);
    }

    @Override
    public void run() {
        String messageContent = new String(message.bytes);

        if (messageContent.startsWith("RUMOUR ")) {
            // first one send
            String[] messageParts = messageContent.split(" ", 2);
            String rumour = messageParts[1];
            try {
                receiveRumour(rumour);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        else if (messageContent.startsWith("RUMOUR_ACK ")) {
            String[] messageParts = messageContent.split(" ", 2);
            String rumourACK = messageParts[1];
            receiveRumourACK(rumourACK);
        }
        else if (messageContent.startsWith("SYNC_TOPOLOGY ")) {
            syncTopology(messageContent);
        }
        else if (messageContent.startsWith("ADD_NODE ")) {
            receiveNewNode(messageContent);
        } else if (messageContent.startsWith("ADD_LB ")) {
            receiveNewLB(messageContent);
        } else if (messageContent.startsWith("ADD_ADMIN ")) {
            receiveNewAdmin(messageContent);
        } else if (messageContent.startsWith("REQUEST_ADD_NODE ")) {
            requestAddNode(messageContent);
        } else if (messageContent.startsWith("REQUEST_ADD_LB ")) {
            requestAddLB(messageContent);
        } else if (messageContent.startsWith("PUT ") || messageContent.startsWith("GET ")) {
            receivePutGet(message);
        } else if (messageContent.startsWith("PUT_ACK")) {
            receiveReply(messageContent, 2);
        } else if (messageContent.startsWith("GET_RESPONSE")) {
            receiveReply(messageContent, 3);
        } else if (messageContent.startsWith("REMOVE ")) {
            try {
                receiveRemoveMessage(messageContent);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
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

        if (messageParts.length > dataLength) {
            String redirectSocketID = messageParts[messageParts.length-1];
            Socket redirectSocket = server.getSocketMap().get(Long.parseLong(redirectSocketID));

            Queue<Message> messageQueue = this.server.getWriteQueue();

            // redirect the message to the socket
            String[] messagesWithoutRedirect = Arrays.copyOfRange(messageParts, 0, messageParts.length - 1);
            String messageContentWithoutRedirect = String.join(" ", messagesWithoutRedirect);

            Message message = new Message(messageContentWithoutRedirect, redirectSocket);
            synchronized (messageQueue) {
                messageQueue.offer(message);
            }
        } else {
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
