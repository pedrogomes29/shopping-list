package Node.ConsistentHashing;

import NioChannels.Socket.Socket;
import Utils.Hasher;

public class TokenNode {
    String id;
    Socket socket;
    public TokenNode(Socket nodesocket, String nodeId){
        this.id = nodeId;
        this.socket = nodesocket;
    }

    public static String[] getVirtualNodesHashes(String nodeId, int nrVirtualNodesPerNode) {
        String[] hashes = new String[nrVirtualNodesPerNode];
        for(int i=0; i < nrVirtualNodesPerNode; i++){
            String virtualNodeId = nodeId + "-" + i;
            String virtualNodeIdHash = Hasher.md5(virtualNodeId);
            hashes[i] = virtualNodeIdHash;
        }
        return hashes;
    }
    public String getId(){
        return this.id;
    }

    public Socket getSocket(){
        return this.socket;
    }
}
