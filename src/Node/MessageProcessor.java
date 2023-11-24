package Node;

import NIOChannels.Message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Queue;

import NIOChannels.Socket;
import Utils.Hasher;
import Utils.MessageBuilder;

public class MessageProcessor extends NIOChannels.MessageProcessor {
    public MessageProcessor(NIOChannels.Server server, Message message) {
        super(server, message);
    }

    private void sendPutACK(String clientID, String objectID ){
        Queue<Message> messageQueue = this.server.getWriteQueue();
        synchronized (messageQueue){
            messageQueue.add(new Message("PUT_ACK" + " " + clientID + " " + objectID,message.getSocket()));
        }
    }

    private void receivePut(String messageContent){
        String[] messageContentStrings = messageContent.split(" ");
        String clientID = messageContentStrings[1];
        String objectID = messageContentStrings[2];
        int nrSpaces = 0;
        int serializedObjectStartingIdx;
        for(serializedObjectStartingIdx=0;serializedObjectStartingIdx<message.bytes.length && nrSpaces<3;serializedObjectStartingIdx++){
            if(message.bytes[serializedObjectStartingIdx]==' ')
                nrSpaces++;
        }
        String hash = null;
        try {
            hash = Hasher.md5(objectID);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] serializedObjectBytes = Arrays.copyOfRange(message.bytes, serializedObjectStartingIdx, message.bytes.length);
        ((Node.Server)server).getDB().insertData(objectID, hash, serializedObjectBytes);

        sendPutACK(clientID,objectID);
    }

    private void sendGetResponse(String clientID, String objectID, byte[] serializedObjectBytes){
        Queue<Message> messageQueue = this.server.getWriteQueue();
        byte[] responseHeader = ("GET_RESPONSE" + " " + clientID + " " + objectID + " ").getBytes();
        byte[] messageBytes = MessageBuilder.buildMessage(responseHeader,serializedObjectBytes);

        synchronized (messageQueue){
            messageQueue.add(new Message(messageBytes,message.getSocket()));
        }
    }

    private void receiveGet(String messageContent){
        String[] messageContentStrings = messageContent.split(" ");
        String clientID = messageContentStrings[1];
        String objectID = messageContentStrings[2];
        byte[] serializedObjectBytes = ((Node.Server)server).getDB().getShoppingList(objectID);
        sendGetResponse(clientID, objectID, serializedObjectBytes);
    }

    @Override
    public void run() {
        String messageContent = new String(message.bytes);
        if(messageContent.startsWith("PUT")) {
            receivePut(messageContent);
        }
        if(messageContent.startsWith("GET")) {
            receiveGet(messageContent);
        }
    }
}
