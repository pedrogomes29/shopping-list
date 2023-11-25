package Node;

import NIOChannels.Message;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Queue;

import ShoppingList.ShoppingListCRDT;
import Utils.Hasher;
import Utils.Serializer;

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
        Object shoppingListCRDT = Serializer.deserializeBase64(messageContentStrings[3]);
        // todo merge crdts

        String hash;
        try {
            hash = Hasher.md5(objectID);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        ((Node.Server)server).getDB().insertData(objectID, hash, Serializer.serializeBase64(shoppingListCRDT));

        sendPutACK(clientID,objectID);
    }

    private void sendGetResponse(String clientID, String objectID, String objectBase64){
        Queue<Message> messageQueue = this.server.getWriteQueue();
        String response = "GET_RESPONSE" + " " + clientID + " " + objectID + " " + objectBase64;

        synchronized (messageQueue){
            messageQueue.add(new Message(response,message.getSocket()));
        }
    }

    private void receiveGet(String messageContent){
        String[] messageContentStrings = messageContent.split(" ");
        String clientID = messageContentStrings[1];
        String objectID = messageContentStrings[2];
        String serializedObjectBase64= ((Node.Server)server).getDB().getShoppingList(objectID);

        sendGetResponse(clientID, objectID, serializedObjectBase64);
    }

    @Override
    public void run() {
        super.run();
        String messageContent = new String(message.bytes);
        if(messageContent.startsWith("PUT ")) {
            receivePut(messageContent);
        }
        if(messageContent.startsWith("GET ")) {
            receiveGet(messageContent);
        }
    }
}
