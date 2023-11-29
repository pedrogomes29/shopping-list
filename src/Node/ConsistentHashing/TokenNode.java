package Node.ConsistentHashing;

import NioChannels.Socket.Socket;
import Utils.Hasher;

import java.security.NoSuchAlgorithmException;

public class TokenNode {
    String id;
    Socket socket;
    public TokenNode(Socket nodesocket, String nodeId){
        this.id = nodeId;
        this.socket = nodesocket;
    }

    public static String[] getVirtualNodesHashes(String nodeId, int nrReplicas) throws NoSuchAlgorithmException {
        String[] hashes = new String[nrReplicas];
        for(int i=0; i < nrReplicas; i++){
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
