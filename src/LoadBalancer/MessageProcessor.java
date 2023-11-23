package LoadBalancer;

import NIOChannels.Message;
import NIOChannels.Socket;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.Queue;

public class MessageProcessor extends NIOChannels.MessageProcessor {
    public MessageProcessor(NIOChannels.Server server, Message message) {
        super(server, message);
    }

    private void addNode(String messageContent){
        String nodeIdStr = messageContent.split(" ")[1];
        TokenNode tokenNode = new TokenNode(message.getSocket(),nodeIdStr,server.getWriteQueue());
        Server loadBalancerServer = (Server) this.server;

        try {
            loadBalancerServer.addNodeToRing(tokenNode);

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendPutACK(String messageContent){
        long clientID = Long.parseLong(messageContent.split(" ")[1]);
        String objectID = messageContent.split(" ")[2];
        Map<Long, Socket> socketMap = this.server.getSocketMap();
        Socket clientSocket = socketMap.get(clientID);
        Queue<Message> writeQueue = server.getWriteQueue();
        synchronized (writeQueue) {
            writeQueue.offer(new Message("PUT_ACK " + " " + objectID,clientSocket));
        }
    }

    private void sendPut(String messageContent) throws NoSuchAlgorithmException {
        String method = "PUT";
        String objectID = messageContent.split(" ")[1];
        long clientID = message.getSocket().getSocketId();
        int objectStartingIdx = method.length() + 1 + objectID.length() + 1;
        byte[] objectBytes = Arrays.copyOfRange(message.bytes, objectStartingIdx, message.bytes.length);
        ((Server)server).sendPut(clientID, objectID, objectBytes);
    }
    private void sendGet(String messageContent) throws NoSuchAlgorithmException {
        String objectID = messageContent.split(" ")[1];
        long clientID = message.getSocket().getSocketId();
        ((Server)server).sendGet(clientID, objectID);
    }

    private void sendGetResponse(String messageContent){
        long clientID = Long.parseLong(messageContent.split(" ")[1]);
        String objectID = messageContent.split(" ")[2];

        byte[] getResponseHeader = ("GET_RESPONSE" + " " + clientID + " " + objectID + " ").getBytes();
        byte[] lineSeparator = System.getProperty("line.separator").getBytes();

        int objectStartingIdx = getResponseHeader.length;
        byte[] objectBytes = Arrays.copyOfRange(message.bytes, objectStartingIdx, message.bytes.length);

        byte[] messageBytes = new byte[getResponseHeader.length + objectBytes.length + lineSeparator.length];

        System.arraycopy(getResponseHeader,0,messageBytes,0,getResponseHeader.length);
        System.arraycopy(objectBytes,0,messageBytes,getResponseHeader.length,objectBytes.length);
        System.arraycopy(lineSeparator,0,messageBytes,getResponseHeader.length+objectBytes.length,lineSeparator.length);


        Map<Long, Socket> socketMap = this.server.getSocketMap();
        Socket clientSocket = socketMap.get(clientID);

        Queue<Message> writeQueue = server.getWriteQueue();
        synchronized (writeQueue) {
           writeQueue.offer(new Message(messageBytes,clientSocket));
        }
    }

    @Override
    public void run() {
        String messageContent = new String(message.bytes);
        if(messageContent.startsWith("ADD_NODE ")) {
            addNode(messageContent);
        }
        else if(messageContent.startsWith("PUT ")){
            try {
                sendPut(messageContent);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
        else if(messageContent.startsWith("PUT_ACK ")){
            sendPutACK(messageContent);
        }

        else if(messageContent.startsWith("GET ")){
            try {
                sendGet(messageContent);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }
        else if(messageContent.startsWith("GET_RESPONSE ")){
            sendGetResponse(messageContent);
        }
    }
}
