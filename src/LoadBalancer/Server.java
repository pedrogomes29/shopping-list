package LoadBalancer;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class Server extends NIOChannels.Server
{

    ArrayList<String> nodeHashes;
    HashMap<String,TokenNode> hashToNode;

    int nrReplicas;

    public Server( int port )
    {
        super(port, new MessageProcessorBuilder());
        nodeHashes = new ArrayList<>();
        hashToNode = new HashMap<>();
        nrReplicas = 3;
    }

    public void addNodeToRing(TokenNode nodeId) throws NoSuchAlgorithmException {
        String nodeIdHash = nodeId.getHash();
        hashToNode.put(nodeIdHash,nodeId);
        int positionToInsert = binarySearch(nodeIdHash);
        nodeHashes.add(positionToInsert,nodeIdHash);
        for (String nodeHash : nodeHashes) System.out.println(nodeHash);
    }

    public static int generateRandomNumber(int min, int max) {
        Random random = new Random();
        return random.ints(min, max)
                .findFirst()
                .getAsInt();
    }

    public void sendPut(Long clientID, String objectID, byte[] object) throws NoSuchAlgorithmException {
        int nrNodes = nodeHashes.size();
        String idHash = Utils.Hasher.md5(objectID);
        int firstNodeToStoreIdx = binarySearch(idHash);
        for(int i=0;i<nrReplicas;i++){
            String nodeHash = nodeHashes.get( (firstNodeToStoreIdx+i)%nrNodes );
            TokenNode node = hashToNode.get(nodeHash);
            node.sendPut(clientID, objectID, object);
        }
    }

    public void sendGet(Long clientID,String objectID) throws NoSuchAlgorithmException {
        int nrNodes = nodeHashes.size();
        String idHash = Utils.Hasher.md5(objectID);
        int firstNodeIdx = binarySearch(idHash);
        int replicaToChoose = generateRandomNumber(firstNodeIdx,firstNodeIdx+nrReplicas);
        String nodeHash = nodeHashes.get( replicaToChoose % nrNodes );
        TokenNode node = hashToNode.get(nodeHash);
        node.sendGet(clientID,objectID);
    }

    private int binarySearch(String hash) {
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

}
