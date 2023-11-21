package LoadBalencer;

import NIOChannels.Message;
import NIOChannels.Socket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Queue;

import static Utils.Hasher.md5;

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

    public void sendPut(String id, Object object){
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            byte[] putHeader = ("PUT" + " " + id + " ").getBytes();

            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(object);
            out.flush();
            byte[] objectBytes = bos.toByteArray();
            byte[] lineSeperator = System.getProperty("line.separator").getBytes();
            byte[] messageBytes = new byte[putHeader.length + objectBytes.length + lineSeperator.length];
            System.arraycopy(putHeader,0,messageBytes,0,putHeader.length);
            System.arraycopy(objectBytes,0,messageBytes,putHeader.length,objectBytes.length);
            System.arraycopy(lineSeperator,0,messageBytes,putHeader.length+objectBytes.length,lineSeperator.length);
            System.out.println(messageBytes.length);
            Message message = new Message(messageBytes, this.socket);
            synchronized (writeQueue) {
                writeQueue.offer(message);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
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
