package LoadBalencer;

import NIOChannels.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;

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


    public void put(String id,Object object) throws NoSuchAlgorithmException {
        int nrNodes = nodeHashes.size();
        String idHash = Utils.Hasher.md5(id);
        int firstNodeToStoreIdx = binarySearch(idHash);
        /*
        TODO: Dynamo stores at first node whose hash is >= hash(id), we are storing at the first larger (not equal)
         */
        for(int i=0;i<nrReplicas;i++){
            String nodeHash = nodeHashes.get( (firstNodeToStoreIdx+i)%nrNodes );
            TokenNode nodeToStore = hashToNode.get(nodeHash);
            nodeToStore.sendPut(id,object);
        }
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
