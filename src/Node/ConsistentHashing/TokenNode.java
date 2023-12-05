package Node.ConsistentHashing;

import NioChannels.Socket.Socket;
import Utils.Hasher;

import java.security.NoSuchAlgorithmException;

public class TokenNode extends Node.Node{
    public TokenNode(Socket nodesocket, String nodeId){
        super(nodesocket,nodeId);
    }

    public static String[] getVirtualNodesHashes(String nodeId, int nrVirtualNodesPerNode) throws NoSuchAlgorithmException {
        String[] hashes = new String[nrVirtualNodesPerNode];
        for(int i=0; i < nrVirtualNodesPerNode; i++){
            String virtualNodeId = nodeId + "-" + i;
            String virtualNodeIdHash = Hasher.md5(virtualNodeId);
            hashes[i] = virtualNodeIdHash;
        }
        return hashes;
    }
}
