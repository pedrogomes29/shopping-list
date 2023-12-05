package Node.ConsistentHashing;

import NioChannels.Message.Message;
import NioChannels.Socket.Socket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

public class ConsistentHashing {

    private final int nrReplicas;
    private final int nrVirtualNodesPerNode;
    private final ArrayList<String> nodeHashes;
    private final HashMap<String, TokenNode> hashToNode;

    public ConsistentHashing(int nrReplicas, int nrVirtualNodesPerNode){
        this.nrReplicas = nrReplicas;
        this.nrVirtualNodesPerNode = nrVirtualNodesPerNode;
        this.nodeHashes = new ArrayList<>();
        this.hashToNode = new HashMap<>();
    }

    public int binarySearch(String hash) {
        int low = 0;
        int high = nodeHashes.size() - 1;

        while (low <= high) {
            int mid = (low + high) / 2;
            String midVal = nodeHashes.get(mid);
            int cmp = midVal.compareTo(hash);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return low;
    }

    public synchronized boolean addNodeToRing(TokenNode node){
        String[] virtualNodeHashes = TokenNode.getVirtualNodesHashes(node.getId(),nrVirtualNodesPerNode);
        for(String virtualNodeHash:virtualNodeHashes) {
            if (hashToNode.containsKey(virtualNodeHash))
                return false;

            hashToNode.put(virtualNodeHash, node);
            int positionToInsert = binarySearch(virtualNodeHash);
            nodeHashes.add(positionToInsert, virtualNodeHash);
        }

        return true;
    }


    public boolean isObjectReplica(String nodeId, String objectId){
        int nrNodes = nodeHashes.size();
        String objectHash = Utils.Hasher.md5(objectId);
        String[] virtualNodeHashes = TokenNode.getVirtualNodesHashes(nodeId,nrVirtualNodesPerNode);

        int firstNodeToStoreIdx = binarySearch(objectHash);

        for(String virtualNodeHash:virtualNodeHashes) {
            for (int i = 0; i < nrReplicas; i++) {
                int idx = (firstNodeToStoreIdx + i) % nrNodes;
                if (nodeHashes.get(idx).equals(virtualNodeHash))
                    return true;
            }
        }

        return false;
    }

    /**
     * Propagates a request to the appropriate node using consistent hashing
     *
     * @param message The message containing the request and object ID.
     * @return The socket of the selected node to handle the request.
     */
    public Socket propagateRequestToNode(Message message){
        int nrNodes = nodeHashes.size();
        String objectID = new String(message.bytes).split(" ")[1];
        String idHash = Utils.Hasher.md5(objectID);
        int firstNodeToStoreIdx = binarySearch(idHash);

        int randomChoice = ThreadLocalRandom.current().nextInt(firstNodeToStoreIdx, firstNodeToStoreIdx+nrReplicas-1);

        System.out.println("Random " + randomChoice + " total " + firstNodeToStoreIdx);
        String nodeHash = nodeHashes.get(randomChoice%nrNodes );
        TokenNode node = hashToNode.get(nodeHash);
        return node.getSocket();
    }

    public String getNthNodeHash(int nodeHashIdx){
        return nodeHashes.get(nodeHashIdx);
    }

    public Socket getNthNodeSocket(int nodeHashIdx){
        String nodeHash = nodeHashes.get(nodeHashIdx);
        return hashToNode.get(nodeHash).getSocket();
    }

    public int getNrNodes(){
        return nodeHashes.size();
    }

    public HashMap<String, TokenNode> getHashToNode() {
        return hashToNode;
    }
}
