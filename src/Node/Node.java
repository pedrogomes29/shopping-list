package Node;

import NioChannels.Socket.Socket;

public class Node {
    String id;
    Socket socket;
    public Node(Socket nodesocket, String nodeId){
        this.id = nodeId;
        this.socket = nodesocket;
    }

    public String getId(){
        return this.id;
    }

    public Socket getSocket(){
        return this.socket;
    }
}
