package Node.ConsistentHashing;

import Node.Message.Message;
import Node.Socket.Socket;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

public class ConsistentHashing {

    private final int nrReplicas;
    private final ArrayList<String> nodeHashes;
    private final HashMap<String, TokenNode> hashToNode;

    public ConsistentHashing(int nrReplicas){
        this.nrReplicas = nrReplicas;
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

    public synchronized boolean addNodeToRing(TokenNode nodeId) throws NoSuchAlgorithmException {
        String nodeIdHash = nodeId.getHash();
        if (hashToNode.containsKey(nodeIdHash) )
            return false;

        hashToNode.put(nodeIdHash,nodeId);
        int positionToInsert = binarySearch(nodeIdHash);

        nodeHashes.add(positionToInsert,nodeIdHash);

        return true;
    }


    public boolean isObjectReplica(String nodeId, String objectId) throws NoSuchAlgorithmException {
        int nrNodes = nodeHashes.size();
        String objectHash = Utils.Hasher.md5(objectId);
        String nodeHash = Utils.Hasher.md5(nodeId);
        int firstNodeToStoreIdx = binarySearch(objectHash);
        System.out.println("objectHash: " + objectHash);
        System.out.println("nodeHash: " + nodeHash);

        for (int i = 0; i < nrReplicas; i++){
            int idx = (firstNodeToStoreIdx+i)%nrNodes;

            System.out.println( idx + " - " + nodeHashes.get(idx));
            if(nodeHashes.get(idx).equals(nodeHash))
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
    public Socket propagateRequestToNode(Message message) throws NoSuchAlgorithmException {
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

    public ArrayList<String> getNodeHashes() {
        return nodeHashes;
    }

    public HashMap<String, TokenNode> getHashToNode() {
        return hashToNode;
    }
}
