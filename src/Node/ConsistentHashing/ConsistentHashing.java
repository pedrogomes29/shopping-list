package Node.ConsistentHashing;

import NioChannels.Message.Message;
import NioChannels.Socket.Socket;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class ConsistentHashing {

    protected final int nrReplicas;
    protected final int nrVirtualNodesPerNode;
    protected final ArrayList<String> nodeHashes;

    protected final HashMap<String, TokenNode> hashToNode;


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

    public synchronized boolean addNodeToRing(TokenNode node) throws NoSuchAlgorithmException {
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

    public synchronized void removeNodeFromRing(TokenNode node) throws NoSuchAlgorithmException {
        String[] virtualNodeHashes = TokenNode.getVirtualNodesHashes(node.getId(),nrVirtualNodesPerNode);
        for(String virtualNodeHash:virtualNodeHashes) {
            if(!hashToNode.containsKey(virtualNodeHash))
                continue;

            hashToNode.remove(virtualNodeHash, node);
            int positionToRemove = binarySearch(virtualNodeHash);
            if(Objects.equals(nodeHashes.get(positionToRemove), virtualNodeHash))
                nodeHashes.remove(positionToRemove);
        }
    }

    public synchronized boolean isObjectReplica(String nodeId, String objectId) throws NoSuchAlgorithmException {
        int nrNodes = nodeHashes.size();
        if(nrNodes<nrReplicas)
            throw new RuntimeException("Not enough replicas");

        String objectHash = Utils.Hasher.md5(objectId);
        String[] virtualNodeHashes = TokenNode.getVirtualNodesHashes(nodeId,nrVirtualNodesPerNode);

        int firstNodeToStoreIdx = binarySearch(objectHash);
        Set<TokenNode> realNodesToPropagateRequest = new HashSet<>();
        Set<String> virtualNodesToPropagateRequest = new HashSet<>();
        for (int i = 0; realNodesToPropagateRequest.size()<nrReplicas && i<nrNodes; i++) {
            int idx = (firstNodeToStoreIdx + i) % nrNodes;
            String currentVirtualNodeHash = nodeHashes.get(idx);
            TokenNode currentRealNode = hashToNode.get(currentVirtualNodeHash);
            if(!realNodesToPropagateRequest.contains(currentRealNode)) {
                virtualNodesToPropagateRequest.add(currentVirtualNodeHash);
                realNodesToPropagateRequest.add(currentRealNode);
            }
        }
        for(String virtualNodeHash:virtualNodeHashes) {
            if (virtualNodesToPropagateRequest.contains(virtualNodeHash))
                return true;
        }

        return false;
    }

    /**
     * Propagates a request to the appropriate node using consistent hashing
     *
     * @param message The message containing the request and object ID.
     * @return The socket of the selected node to handle the request.
     * @throws NoSuchAlgorithmException If the required hashing algorithm is not available.
     */
    public synchronized Socket propagateRequestToNode(Message message) throws NoSuchAlgorithmException {
        int nrNodes = nodeHashes.size();
        if(nrNodes<nrReplicas)
            throw new RuntimeException("Not enough replicas");
        String objectID = new String(message.bytes).split(" ")[1];
        String idHash = Utils.Hasher.md5(objectID);
        int firstNodeToStoreIdx = binarySearch(idHash);
        Set<TokenNode> realNodesToPropagateRequest = new HashSet<>();
        int[] choicesIdx = new int[nrReplicas];
        for(int idxOffset=0;realNodesToPropagateRequest.size()<nrReplicas && idxOffset<nrNodes-1;idxOffset++){
            int idx = (firstNodeToStoreIdx+idxOffset)%nrNodes;
            String currentVirtualNodeHash = nodeHashes.get(idx);
            TokenNode currentRealNode = hashToNode.get(currentVirtualNodeHash);
            if(!realNodesToPropagateRequest.contains(currentRealNode)){
                choicesIdx[realNodesToPropagateRequest.size()] = idx;
                realNodesToPropagateRequest.add(currentRealNode);
            }
        }
        int randomChoice = ThreadLocalRandom.current().nextInt(0, nrReplicas);
        System.out.println("Random " + randomChoice + " total " + firstNodeToStoreIdx);
        String nodeHash = nodeHashes.get(choicesIdx[randomChoice]);
        TokenNode node = hashToNode.get(nodeHash);
        return node.getSocket();
    }

    public synchronized String getNthNodeHash(int nodeHashIdx){
        return nodeHashes.get(nodeHashIdx);
    }

    public synchronized TokenNode getNthNode(int nodeHashIdx){
        String nodeHash = nodeHashes.get(nodeHashIdx);
        return hashToNode.get(nodeHash);
    }

    public synchronized int getNrNodes(){
        return nodeHashes.size();
    }

    public synchronized HashMap<String, TokenNode> getHashToNode() {
        return hashToNode;
    }

    public synchronized int getNrRealNodes(){
        return getNrNodes()/nrVirtualNodesPerNode;
    }

    public synchronized Set<String> getNodes() {
        Set<String> nodes = new HashSet<>();
        for (TokenNode tokenNode: hashToNode.values()) {
            nodes.add(tokenNode.getId());
        }
        return nodes;
    }
}
