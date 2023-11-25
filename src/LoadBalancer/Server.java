package LoadBalancer;

import Node.Message.Message;
import Node.Socket.Socket;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class Server extends Node.Server
{
    public Server(String confFilePath, int port ) throws IOException {
        super(confFilePath, port, new MessageProcessorBuilder());
        neighborIDs.add(this.nodeId);
        addRumour("ADD_LB" + " " + nodeId + " " + port );
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

        String messageContent = new String(requestMsg.bytes);

        String[] requestParts = messageContent.split(" ");

        StringBuilder stringBuilder = new StringBuilder(requestParts[0] + " " + clientID);

        for (int i = 1; i < requestParts.length; i++)
            stringBuilder.append(" ").append(requestParts[i]);

        synchronized (outboundMessageQueue) {
            outboundMessageQueue.offer(new Message(stringBuilder.toString(),nodeSocket));
        }
    }

    public void propagateResponseToClient(byte[] request){
        String messageContent = new String(request);
        String[] requestParts = messageContent.split(" ");

        // get client
        long clientID = Long.parseLong(requestParts[1]);
        Socket clientSocket = socketMap.get(clientID);

        // build message without clientID
        String requestMethod = requestParts[0];
        StringBuilder stringBuilder = new StringBuilder(requestMethod);

        for (int i = 2; i < requestParts.length; i++)
            stringBuilder.append(" ").append(requestParts[i]);

        synchronized (outboundMessageQueue) {
            outboundMessageQueue.offer(new Message(stringBuilder.toString(),clientSocket));
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
