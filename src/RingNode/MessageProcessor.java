package RingNode;

import Database.Database;
import NioChannels.Message.Message;

import java.util.*;

import NioChannels.Socket.Socket;
import ShoppingList.ShoppingListCRDT;
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
        ShoppingListCRDT shoppingListCRDTFromClient = (ShoppingListCRDT)Serializer.deserializeBase64(messageContentParts[2]);

        Database db = ((RingNode.Server)server).getDB();

        String shoppingListCRDTInDatabaseBase64 = db.getShoppingList(objectID);
        ShoppingListCRDT shoppingListCRDT = new ShoppingListCRDT();
        if(shoppingListCRDTInDatabaseBase64!=null) {
            shoppingListCRDT = (ShoppingListCRDT) Serializer.deserializeBase64(shoppingListCRDTInDatabaseBase64);
        }

        shoppingListCRDT.merge(shoppingListCRDTFromClient);

        String objectIDHash,objectHash;
        objectIDHash = Hasher.md5(objectID);
        objectHash = Hasher.encodeAndMd5(shoppingListCRDT);

        db.insertData(objectID, objectIDHash, Serializer.serializeBase64(shoppingListCRDT),objectHash);

        sendPutACK(message, objectID, messageContentParts);
    }

    private void sendGetResponse(Message message, String objectID, String objectBase64, String[] messageContentParts){
        String response = "GET_RESPONSE " + objectID + " " + objectBase64;

        response = addRedirects(response, messageContentParts, 2);
        Message responseMessage = new Message(response,message.getSocket());
        Queue<Message> messageQueue = this.server.getWriteQueue();
        synchronized (messageQueue){
            messageQueue.add(responseMessage);
        }
    }

    private void receiveGet(Message message, String[] messageContentParts){
        String objectID = messageContentParts[1];
        String serializedObjectBase64= ((RingNode.Server)server).getDB().getShoppingList(objectID);

        sendGetResponse(message, objectID, serializedObjectBase64, messageContentParts);
    }


    private void receiveSynchronize(Message message, String[] messageContentParts){

        String startingHash = messageContentParts[1];
        String endingHash = messageContentParts[2];
        StringBuilder synchronizationMessageBuilder = new StringBuilder("SYNCHRONIZE_DIFF" + " ");
        Map<String, String> shoppingListsBase64 = ((RingNode.Server)server).getDB().
                getShoppingListsBase64(startingHash, endingHash);
        if(messageContentParts.length>=4) {
            String objects = messageContentParts[3];
            String[] objectsParts = objects.split(",");


            for (String object : objectsParts) {
                String[] objectParts = object.split(":");
                String objectID = objectParts[0];

                if(! (((Server) server).consistentHashing.isObjectReplica(((Server) server).getNodeId(),objectID)))
                    continue;

                shoppingListsBase64.remove(objectID);
                String otherReplicasObjectHash = objectParts[1];
                String thisReplicaObjectHash = ((RingNode.Server) server).getDB().getShoppingListHash(objectID);
                String thisReplicaObjectBase64 = ((RingNode.Server) server).getDB().getShoppingList(objectID);


                if (thisReplicaObjectHash == null || !thisReplicaObjectHash.equals(otherReplicasObjectHash)) { //object isn't equal between replicas
                    synchronizationMessageBuilder.append(objectID).append(":").append(thisReplicaObjectBase64).append(',');
                }
            }
        }

        for(String objectID:shoppingListsBase64.keySet()){ //objects in this replica but not in the message sender replica
            String objectBase64 = ((RingNode.Server)server).getDB().getShoppingList(objectID);
            synchronizationMessageBuilder.append(objectID).append(":").append(objectBase64).append(',');
        }

        if (synchronizationMessageBuilder.toString().endsWith(",")) {
            synchronizationMessageBuilder.setLength(synchronizationMessageBuilder.length() - 1); //remove last ','
        }
        else
            return; //no diff, no need to send message


        String synchronizationMessage = synchronizationMessageBuilder.toString();
        Message responseMessage = new Message(synchronizationMessage,message.getSocket());
        Queue<Message> messageQueue = this.server.getWriteQueue();
        synchronized (messageQueue){
            messageQueue.add(responseMessage);
        }
    }



    private void synchronizeShoppingList(String objectID, String thisReplicaObjectBase64, String otherReplicaObjectBase64){
        if(Objects.equals(otherReplicaObjectBase64, "null")) //the other node didnt send anything
            return;
        ShoppingListCRDT shoppingListCRDT;
        ShoppingListCRDT otherReplicaShoppingList = (ShoppingListCRDT) Serializer.deserializeBase64(otherReplicaObjectBase64);

        if(thisReplicaObjectBase64==null) { //new object, simply store it
            shoppingListCRDT = new ShoppingListCRDT();
        }else {
            shoppingListCRDT = (ShoppingListCRDT) Serializer.deserializeBase64(thisReplicaObjectBase64);
        }

        shoppingListCRDT.merge(otherReplicaShoppingList);

        String objectIDHash,objectHash;
        objectIDHash = Hasher.md5(objectID);
        objectHash = Hasher.encodeAndMd5(shoppingListCRDT);


        ((RingNode.Server)server).getDB().insertData(objectID, objectIDHash,
                                                     Serializer.serializeBase64(shoppingListCRDT),objectHash);
    }

    private void receiveSynchronizeDiff(Message message, String[] messageContentParts) {
        String objects = messageContentParts[1];
        String[] objectsParts = objects.split(",");
        StringBuilder synchronizationMessageBuilder = new StringBuilder("SYNCHRONIZE_RESPONSE" + " ");

        for(String object:objectsParts){
            String[] objectParts = object.split(":");

            String objectID = objectParts[0];
            if(! (((Server) server).consistentHashing.isObjectReplica(((Server) server).getNodeId(),objectID)))
                continue;
            String otherReplicaObjectBase64 = objectParts[1];

            String thisReplicaObjectBase64 = ((RingNode.Server)server).getDB().getShoppingList(objectID);

            if(thisReplicaObjectBase64!=null) //the other node needs a new version of this object
                synchronizationMessageBuilder.append(objectID).append(":").append(thisReplicaObjectBase64).append(',');

            synchronizeShoppingList(objectID,thisReplicaObjectBase64,otherReplicaObjectBase64);
        }

        if (synchronizationMessageBuilder.toString().endsWith(",")) {
            synchronizationMessageBuilder.setLength(synchronizationMessageBuilder.length() - 1); //remove last ','
        }
        else
            return; //empty response, no need to send message

        String synchronizationMessage = synchronizationMessageBuilder.toString();
        Message responseMessage = new Message(synchronizationMessage,message.getSocket());
        Queue<Message> messageQueue = this.server.getWriteQueue();
        synchronized (messageQueue){
            messageQueue.add(responseMessage);
        }
    }


    private void receiveSynchronizeResponse(Message message, String[] messageContentParts) {
        String objects = messageContentParts[1];
        String[] objectsParts = objects.split(",");
        for(String object:objectsParts){
            String[] objectParts = object.split(":");

            String objectID = objectParts[0];
            if(! (((Server) server).consistentHashing.isObjectReplica(((Server) server).getNodeId(),objectID)))
                continue;
            String otherReplicaObjectBase64 = objectParts[1];
            String thisReplicaObjectBase64 = ((RingNode.Server)server).getDB().getShoppingList(objectID);

            synchronizeShoppingList(objectID,thisReplicaObjectBase64,otherReplicaObjectBase64);
        }
    }



    @Override
    protected Socket isToRedirectMessage(Message message) {
        String messageContent = new String(message.bytes);
        String[] messageContentParts = messageContent.split(" ");
        String messageType = messageContentParts[0];
        String objectID = messageContentParts[1];
        if(getServer().consistentHashing.isObjectReplica(getServer().getNodeId(), objectID)){
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
            return getServer().consistentHashing.propagateRequestToNode(message);
        }

        return null;
    }

    @Override
    public void run() {
        super.run();
        String messageContent = new String(message.bytes);
        String[] messageContentParts = messageContent.split(" ");
        String messageType = messageContentParts[0];
        switch(messageType) {
            case "SYNCHRONIZE":
                receiveSynchronize(message, messageContentParts);
                break;
            case "SYNCHRONIZE_DIFF":
                receiveSynchronizeDiff(message, messageContentParts);

                break;
            case "SYNCHRONIZE_RESPONSE":
                receiveSynchronizeResponse(message, messageContentParts);
                break;
        }
    }


}
