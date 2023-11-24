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
    String hash;
    public TokenNode(Socket nodesocket, String nodeId){
        try {
            this.id = nodeId;
            this.socket = nodesocket;
            this.hash =  Utils.Hasher.md5(nodeId);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
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
