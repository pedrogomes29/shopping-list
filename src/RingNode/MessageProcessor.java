package RingNode;

import NioChannels.Message.Message;

import java.util.*;
import java.sql.SQLException;
import java.util.Queue;

import NioChannels.Socket.Socket;
import ShoppingList.ShoppingListCRDT;
import Utils.Hasher;
import Utils.Serializer;

public class MessageProcessor extends Node.Message.MessageProcessor {
    public MessageProcessor(Server server, Message message) {
        super(server, message);
    }

    /**
     * Appends the redirects id's of the original message (messageParts) to the newMessage.
     *
     * @param newMessage The new message.
     * @param messageParts An array of string parts of the original message
     * @param startRedirects The index from which the parts correspond to the id's of the computers to redirect the message
     * @return a new string message with the newMessage and all redirect's ids.
     */
    private String addRedirects(String newMessage, String[] messageParts, int startRedirects){
        StringBuilder stringBuilder = new StringBuilder(newMessage);
        for (int i = startRedirects; i < messageParts.length; i++)
            stringBuilder.append(" ").append(messageParts[i]);

        return stringBuilder.toString();
    }

    /**
     * Sends a PUT_ACK message to the client in response to a successful PUT request.
     * @param message The original message received from the client.
     * @param objectID The identifier of the object associated with the PUT request.
     * @param messageParts Additional message parts to be included in the response.
     */
    private void sendPutACK(Message message, String objectID, String[] messageParts ){
        Queue<Message> messageQueue = this.server.getWriteQueue();

        String ackMessage = addRedirects("PUT_ACK " + objectID, messageParts, 3);
        System.out.println("Message " + ackMessage);
        Message putAckMessage = new Message(ackMessage,message.getSocket());
        synchronized (messageQueue){
            messageQueue.add(putAckMessage);
        }
    }

    /**
     * Sends a PUT_NACK message to the client in response to a failed PUT request.
     * @param message The original message received from the client.
     * @param objectID The identifier of the object associated with the PUT request.
     * @param messageParts Additional message parts to be included in the response.
     */
    private void sendPutNACK(Message message, String objectID, String[] messageParts , String nackErrorMessage){
        Queue<Message> messageQueue = this.server.getWriteQueue();

        String nackMessage = addRedirects("PUT_NACK " + objectID + " " + nackErrorMessage, messageParts, 3);
        Message putAckMessage = new Message(nackMessage,message.getSocket());
        synchronized (messageQueue){
            messageQueue.add(putAckMessage);
        }
    }

    /**
     * Handles the processing of a "PUT" message, responsible for receiving and storing a
     * ShoppingListCRDT object from a client. This method performs the following steps:
     * <p>
     * Checks the success of deserialization, and sends a NACK with message error_deserialize to the client if it fails.
     * Sends a positive acknowledgment (ACK) to the client upon successful storage in the database.
     * If cant insert to the db sends a NACK with the message error_storing_db
     *
     * @param message               The incoming message containing the PUT request.
     * @param messageContentParts   Array containing different parts of the message content.
     *                              Index 1: Object ID
     *                              Index 2: Serialized ShoppingListCRDT
     */
    private void receivePut(Message message, String[] messageContentParts){
        String objectID = messageContentParts[1];
        ShoppingListCRDT shoppingListCRDT = (ShoppingListCRDT) Serializer.deserializeBase64(messageContentParts[2]);
        if (shoppingListCRDT == null){
            System.err.println("Warning: deserializeBase64 CRDT");
            sendPutNACK(message, objectID, messageContentParts, "error_deserialize");
            return;
        }

        // todo merge crdts
        System.out.println("merge crdts");

        // send confirmation to client
        String hash = Hasher.md5(objectID);
        try {
            ((Server)server).getDB().insertData(objectID, hash, Serializer.serializeBase64(shoppingListCRDT));
            sendPutACK(message, objectID, messageContentParts);
        } catch (SQLException e) {
            System.err.println("Error: cant insert object into database. objectID" + objectID);
            e.printStackTrace(System.err);
            sendPutNACK(message, objectID, messageContentParts, "error_storing_db");
        }

    }

    /**
     * Sends a GET response to the client with the specified parameters.
     *
     * This method constructs a GET_RESPONSE message containing the provided objectID,
     * objectBase64, and additional messageContentParts. The constructed message may
     * include redirects based on the messageContentParts. The response message is then
     * added to the server's write queue for sending to the client.
     *
     * @param message The original message received from the client.
     * @param objectID The identifier of the object for the GET response.
     * @param objectBase64 The Base64-encoded representation of the object for the GET response.
     * @param messageContentParts Additional content parts for constructing the response.
     * @see Message
     */
    private void sendGetResponse(Message message, String objectID, String objectBase64, String[] messageContentParts){
        String response = "GET_RESPONSE " + objectID + " " + objectBase64;

        response = addRedirects(response, messageContentParts, 2);
        Message responseMessage = new Message(response,message.getSocket());
        Queue<Message> messageQueue = this.server.getWriteQueue();
        synchronized (messageQueue){
            messageQueue.add(responseMessage);
        }
    }

    /**
     * Handles the processing of a "GET" message, responsible for retrieving a ShoppingListCRDT
     * object from the server and sending the serialized response back to the client.
     * <p>
     * If he retrieval is successful, sends a response to the client with the serialized object.
     * Otherwise, if there are issues with the database (SQLException) or if the object is not found (NullPointerException),
     * sends a GET_NACK with the messages error_database or error_null
     *
     * @param message               The incoming message containing the GET request.
     * @param messageContentParts   Array containing different parts of the message content.
     *                              Index 1: Object ID
     */
    private void receiveGet(Message message, String[] messageContentParts){
        String objectID = messageContentParts[1];
        try {
            String serializedObjectBase64 = ((RingNode.Server) server).getDB().getShoppingList(objectID);
            if (serializedObjectBase64 == null)
                throw new NullPointerException();
            sendGetResponse(message, objectID, serializedObjectBase64, messageContentParts);

        }catch (SQLException e) {
            System.err.println("Error: SQLException receiveGet with id " + objectID);
            sendGetNACK(message, objectID, messageContentParts, "error_database");
        }catch (NullPointerException e){
            System.err.println("Error: NullPointerException receiveGet with id " + objectID);
            sendGetNACK(message, objectID, messageContentParts, "error_null");
        }
    }

    /**
     * Sends a GET_NACK (Negative Acknowledgment) message to the client, indicating an error
     * during a GET request. Appends additional message parts to the basic message format.
     *
     * @param message The original message associated with the GET request.
     * @param objectID The identifier of the object for which the GET operation failed.
     * @param messageParts Additional parts of the message to be appended after the basic format.
     * @param errorMessage The specific error message or code related to the database error.
     */
    private void sendGetNACK(Message message, String objectID, String[] messageParts, String errorMessage) {
        Queue<Message> messageQueue = this.server.getWriteQueue();
        String nackMessage = addRedirects("GET_NACK " + objectID + " " + errorMessage, messageParts, 2);
        Message putAckMessage = new Message(nackMessage,message.getSocket());
        synchronized (messageQueue){
            messageQueue.add(putAckMessage);
        }
    }

    
    private void receiveSynchronize(Message message, String[] messageContentParts) throws SQLException {

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
                shoppingListsBase64.remove(objectID);
                String otherReplicaObjectBase64Hash = objectParts[1];
                String thisReplicaObjectBase64 = null;

                thisReplicaObjectBase64 = ((Server) server).getDB().getShoppingList(objectID);


                String thisReplicaObjectBase64Hash = null;
                if (thisReplicaObjectBase64 != null)
                    thisReplicaObjectBase64Hash = Hasher.md5(thisReplicaObjectBase64);

                if (thisReplicaObjectBase64Hash == null || !thisReplicaObjectBase64Hash.equals(otherReplicaObjectBase64Hash)) { //object isn't equal between replicas
                    synchronizationMessageBuilder.append(objectID).append(":").append(thisReplicaObjectBase64).append(',');
                }
            }
        }

        for(String objectID:shoppingListsBase64.keySet()){ //objects in this replica but not in the message sender replica
            String objectBase64 = ((RingNode.Server)server).getDB().getShoppingList(objectID);
            synchronizationMessageBuilder.append(objectID).append(":").append(objectBase64).append(',');
        }

        sendSyncronizationMessage(message, synchronizationMessageBuilder);
    }

    private void sendSyncronizationMessage(Message message, StringBuilder synchronizationMessageBuilder) {
        if (synchronizationMessageBuilder.toString().endsWith(","))
            synchronizationMessageBuilder.setLength(synchronizationMessageBuilder.length() - 1); //remove last ','
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
        if(!Objects.equals(otherReplicaObjectBase64, "null")){ //the other node has a different version of this object
            ShoppingListCRDT shoppingListCRDT;
            if(thisReplicaObjectBase64==null) //new object, simply store it
                shoppingListCRDT = (ShoppingListCRDT)Serializer.deserializeBase64(otherReplicaObjectBase64);
            else { //different object version, merge it
                ShoppingListCRDT otherReplicaShoppingList = (ShoppingListCRDT) Serializer.deserializeBase64(otherReplicaObjectBase64);
                shoppingListCRDT = (ShoppingListCRDT) Serializer.deserializeBase64(thisReplicaObjectBase64);
                shoppingListCRDT.merge(otherReplicaShoppingList);
            }

            String hash = Hasher.md5(objectID);

            try {
                ((RingNode.Server)server).getDB().insertData(objectID, hash, Serializer.serializeBase64(shoppingListCRDT));
            } catch (SQLException e) {
                System.err.println("Error: cant insert object into database. objectID" + objectID);
                e.printStackTrace(System.err);
            }

        }
    }

    private void receiveSynchronizeDiff(Message message, String[] messageContentParts) throws SQLException {
        String objects = messageContentParts[1];
        String[] objectsParts = objects.split(",");
        StringBuilder synchronizationMessageBuilder = new StringBuilder("SYNCHRONIZE_RESPONSE" + " ");

        for(String object:objectsParts){
            String[] objectParts = object.split(":");

            String objectID = objectParts[0];
            String otherReplicaObjectBase64 = objectParts[1];

            String thisReplicaObjectBase64 = ((RingNode.Server)server).getDB().getShoppingList(objectID);

            if(thisReplicaObjectBase64!=null) //the other node needs a new version of this object
                synchronizationMessageBuilder.append(objectID).append(":").append(thisReplicaObjectBase64).append(',');

            synchronizeShoppingList(objectID,thisReplicaObjectBase64,otherReplicaObjectBase64);
        }

        sendSyncronizationMessage(message, synchronizationMessageBuilder);
    }


    private void receiveSynchronizeResponse(Message message, String[] messageContentParts) throws SQLException {
        String objects = messageContentParts[1];
        String[] objectsParts = objects.split(",");
        for(String object:objectsParts){
            String[] objectParts = object.split(":");

            String objectID = objectParts[0];
            String otherReplicaObjectBase64 = objectParts[1];

            String thisReplicaObjectBase64 = ((RingNode.Server)server).getDB().getShoppingList(objectID);
            synchronizeShoppingList(objectID,thisReplicaObjectBase64,otherReplicaObjectBase64);

        }
    }
    

    /**
     * Determine whether to redirect a given message based on its content.
     * <p>
     * This function should only receive PUT or GET messages.
     * <p>
     * If the object id in this consistentHashing ring, call functions to handle the put or get messages
     * Otherwise, redirect to the correct node.
     *
     * @param message The message to be evaluated for redirection.
     * @return A Socket for redirection if needed, otherwise null.
     */
    @Override
    protected Socket isToRedirectMessage(Message message) {
        String messageContent = new String(message.bytes);
        String[] messageContentParts = messageContent.split(" ");
        String messageType = messageContentParts[0];

        if(!messageType.startsWith("PUT") && !messageContent.startsWith("GET")) {
            System.err.println("MessageType unknown " + messageType);
            return null;
        }

        String objectID = messageContentParts[1];

        if(getServer().consistentHashing.isObjectReplica(getServer().getNodeId(), objectID)){
            switch (messageType){
                case "PUT":
                    receivePut(message, messageContentParts);
                    break;
                case "GET":
                    receiveGet(message, messageContentParts);
                    break;
            }
            return null;
        }

        return getServer().consistentHashing.propagateRequestToNode(message);
    }

    @Override
    public void run() {
        super.run();
        String messageContent = new String(message.bytes);
        String[] messageContentParts = messageContent.split(" ");
        String messageType = messageContentParts[0];
        try {
            switch (messageType) {
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
        }catch (SQLException e) {
            System.err.println("Error: Invalid SQLQuery " + e.getMessage());
        }
    }


}
