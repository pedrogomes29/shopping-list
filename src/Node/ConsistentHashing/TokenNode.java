package Node.ConsistentHashing;

import NioChannels.Socket.Socket;
import Utils.Hasher;

import java.net.InetSocketAddress;

public class TokenNode extends Node.Node{
    private boolean active;

    public TokenNode(Socket nodesocket, String nodeId, InetSocketAddress nodeEndpoint){
        super(nodesocket,nodeId,nodeEndpoint);
        active = true;
    }

    public static String[] getVirtualNodesHashes(String nodeId, int nrVirtualNodesPerNode)  {
        String[] hashes = new String[nrVirtualNodesPerNode];
        for(int i=0; i < nrVirtualNodesPerNode; i++){
            String virtualNodeId = nodeId + "-" + i;
            String virtualNodeIdHash = Hasher.md5(virtualNodeId);
            hashes[i] = virtualNodeIdHash;
        }
        return hashes;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean isActive(){
        return active;
    }

}