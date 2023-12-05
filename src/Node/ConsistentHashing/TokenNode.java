package Node.ConsistentHashing;

import NioChannels.Socket.Socket;
import Utils.Hasher;

import java.net.InetSocketAddress;

public class TokenNode extends Node.Node{
    public TokenNode(Socket nodesocket, String nodeId, InetSocketAddress nodeEndpoint){
        super(nodesocket,nodeId,nodeEndpoint);
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
}
