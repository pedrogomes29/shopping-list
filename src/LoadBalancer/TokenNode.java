package LoadBalancer;

import NIOChannels.Message;
import NIOChannels.Socket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Queue;

public class TokenNode {
    String id;
    Socket socket;

    final Queue<Message> writeQueue;

    String hash;
    public TokenNode(Socket nodesocket, String nodeId,Queue<Message> writeQueue){
        try {
            this.id = nodeId;
            this.socket = nodesocket;
            this.hash =  Utils.Hasher.md5(nodeId);
            this.writeQueue = writeQueue;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public void sendPut(Long clientID,String objectID, byte[] objectBytes){
        byte[] putHeader = ("PUT" + " " + clientID + " " + objectID + " ").getBytes();
        byte[] lineSeparator = System.getProperty("line.separator").getBytes();
        byte[] messageBytes = new byte[putHeader.length + objectBytes.length + lineSeparator.length];

        System.arraycopy(putHeader,0,messageBytes,0,putHeader.length);
        System.arraycopy(objectBytes,0,messageBytes,putHeader.length,objectBytes.length);
        System.arraycopy(lineSeparator,0,messageBytes,putHeader.length+objectBytes.length,lineSeparator.length);

        Message message = new Message(messageBytes, socket);
        synchronized (writeQueue) {
            writeQueue.offer(message);
        }
    }

    public void sendGet(Long clientID,String objectID){
        Message message = new Message("GET" + " " + clientID + " " + objectID, this.socket);
        synchronized (writeQueue) {
            writeQueue.offer(message);
        }
    }

    public String getId(){
        return this.id;
    }

    public Socket getSocket(){
        return this.socket;
    }

    public String getHash(){
        return hash;
    }
}
