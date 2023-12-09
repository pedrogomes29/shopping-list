package Node;

import NioChannels.Socket.Socket;

import java.net.InetSocketAddress;

public class Node {
    String id;
    InetSocketAddress nodeEndpoint;
    Socket socket;
    public Node(Socket nodesocket, String nodeId, InetSocketAddress nodeEndpoint){
        this.id = nodeId;
        this.socket = nodesocket;
        this.nodeEndpoint = nodeEndpoint;
    }

    public String getId(){
        return this.id;
    }

    public Socket getSocket(){
        return this.socket;
    }

    public InetSocketAddress getNodeEndpoint(){
        return nodeEndpoint;
    }
}
