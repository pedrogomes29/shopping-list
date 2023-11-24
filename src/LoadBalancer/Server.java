package LoadBalancer;

import NIOChannels.Message;
import NIOChannels.Socket;
import Utils.MessageBuilder;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
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

    public void  propagateRequestToNode(Message message) throws NoSuchAlgorithmException {
        int nrNodes = nodeHashes.size();
        String objectID = new String(message.bytes).split(" ")[1];
        String idHash = Utils.Hasher.md5(objectID);
        int firstNodeToStoreIdx = binarySearch(idHash);
        for(int i=0;i<nrReplicas;i++){
            String nodeHash = nodeHashes.get( (firstNodeToStoreIdx+i)%nrNodes );
            TokenNode node = hashToNode.get(nodeHash);
            propagateRequestWithClientId(message,node.getSocket());
        }
    }

    public void propagateRequestWithClientId(Message requestMsg, Socket nodeSocket){
        long clientID = requestMsg.getSocket().getSocketId();

        byte[] request = requestMsg.bytes;
        String messageContent = new String(request);

        String[] requestParts = messageContent.split(" ");
        String requestMethod = requestParts[0];

        int requestBodyStartIdx = requestMethod.length() + 1;
        byte[] requestHeader = (requestMethod + " ").getBytes();
        byte[] requestBody = Arrays.copyOfRange(request,requestBodyStartIdx,request.length);
        byte[] messageBytes = MessageBuilder.buildMessage(requestHeader,requestBody);

        synchronized (outboundMessageQueue) {
            outboundMessageQueue.offer(new Message(messageBytes,nodeSocket));
        }
    }

    public void propagateResponseToNode(byte[] request){
        String messageContent = new String(request);
        String[] requestParts = messageContent.split(" ");
        String requestMethod = requestParts[0];
        String clientIDStr = requestParts[1];
        long clientID = Long.parseLong(clientIDStr);

        int requestBodyStartIdx = requestMethod.length() + 1 + clientIDStr.length() + 1;

        byte[] requestHeader = (requestMethod + " ").getBytes();
        byte[] requestBody = Arrays.copyOfRange(request,requestBodyStartIdx,request.length);
        byte[] messageBytes = MessageBuilder.buildMessage(requestHeader,requestBody);

        Socket clientSocket = socketMap
                .get(clientID);

        synchronized (outboundMessageQueue) {
            outboundMessageQueue.offer(new Message(messageBytes,clientSocket));
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
