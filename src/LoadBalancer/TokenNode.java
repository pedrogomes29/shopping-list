package LoadBalancer;

import Node.Socket.Socket;

import java.security.NoSuchAlgorithmException;

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
