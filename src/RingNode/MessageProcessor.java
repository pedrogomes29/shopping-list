package RingNode;

import Node.Message.Message;

import java.security.NoSuchAlgorithmException;
import java.util.Queue;

import Node.Socket.Socket;
import Utils.Hasher;
import Utils.Serializer;

public class MessageProcessor extends Node.Message.MessageProcessor {
    public MessageProcessor(Server server, Message message) {
        super(server, message);
    }

    private String addRedirects(String inicialString, String[] messageParts, int startRedirects){
        StringBuilder stringBuilder = new StringBuilder(inicialString);
        for (int i = startRedirects; i < messageParts.length; i++)
            stringBuilder.append(" ").append(messageParts[i]);

        return stringBuilder.toString();
    }
    private void sendPutACK(Message message, String objectID, String[] messageParts ){
        Queue<Message> messageQueue = this.server.getWriteQueue();

        String ackMessage = addRedirects("PUT_ACK " + objectID, messageParts, 3);
        System.out.println("Message " + ackMessage);
        Message putAckMessage = new Message(ackMessage,message.getSocket());
        synchronized (messageQueue){
            messageQueue.add(putAckMessage);
        }
    }

    private void receivePut(Message message, String[] messageContentParts){
        String objectID = messageContentParts[1];
        Object shoppingListCRDT = Serializer.deserializeBase64(messageContentParts[2]);
        // todo merge crdts
        System.out.println("merge crdts");

        String hash;
        try {
            hash = Hasher.md5(objectID);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        ((RingNode.Server)server).getDB().insertData(objectID, hash, Serializer.serializeBase64(shoppingListCRDT));

        sendPutACK(message, objectID, messageContentParts);
    }

    private void sendGetResponse(Message message, String objectID, String objectBase64, String[] messageContentParts){
        Queue<Message> messageQueue = this.server.getWriteQueue();
        String response = "GET_RESPONSE " + objectID + " " + objectBase64;

        response = addRedirects(response, messageContentParts, 2);
        Message responseMessage = new Message(response,message.getSocket());
        synchronized (messageQueue){
            messageQueue.add(responseMessage);
        }
    }

    private void receiveGet(Message message, String[] messageContentParts){
        String objectID = messageContentParts[1];
        String serializedObjectBase64= ((RingNode.Server)server).getDB().getShoppingList(objectID);

        sendGetResponse(message, objectID, serializedObjectBase64, messageContentParts);
    }


    @Override
    protected Socket isToRedirectMessage(Message message) {
        String messageContent = new String(message.bytes);
        String[] messageContentParts = messageContent.split(" ");
        String messageType = messageContentParts[0];
        String objectID = messageContentParts[1];

        try {
            if(server.isObjectReplica(this.server.getNodeId(), objectID)){
                System.out.println("e replica");
                switch (messageType){
                    case "PUT":
                        receivePut(message, messageContentParts);
                        break;
                    case "GET":
                        receiveGet(message, messageContentParts);
                        break;
                    default:
                        throw new RuntimeException("MessageType unknown " + messageType);
                }
            }else {
                System.out.println("nao e");
                try {
                    return server.propagateRequestToNode(message);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        return null;
    }
}
