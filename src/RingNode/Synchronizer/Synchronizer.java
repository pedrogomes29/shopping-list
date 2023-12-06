package RingNode.Synchronizer;

import NioChannels.Message.Message;
import Node.ConsistentHashing.TokenNode;
import RingNode.Server;
import NioChannels.Socket.Socket;
import Utils.Hasher;

import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Synchronizer implements Runnable{
    private final int nrReplicas;
    private final int nrVirtualNodesPerNode;
    private final Queue<Message> writeQueue;
    private final Server server;
    private final static int nrSecondsBetweenSynchronization = 5;


    public Synchronizer(Server server, Queue<Message> writeQueue, int nrReplicas, int nrVirtualNodesPerNode){
        this.writeQueue = writeQueue;
        this.server = server;
        this.nrReplicas = nrReplicas;
        this.nrVirtualNodesPerNode = nrVirtualNodesPerNode;
    }

    @Override
    public void run() {
        while(server.running) {
            String[] virtualNodeHashes = TokenNode.getVirtualNodesHashes(server.getNodeId(),nrVirtualNodesPerNode);

            int nrNodes = server.consistentHashing.getNrNodes();


            for(String virtualNodeHash:virtualNodeHashes) {

                int virtualNodeIdx = server.consistentHashing.binarySearch(virtualNodeHash);
                List<Integer> availableOffsets = new ArrayList<>();

                for (int i=-nrReplicas+1;i<0;i++) {
                    availableOffsets.add(i);
                }

                for (int i=1;i<nrReplicas;i++) {
                    availableOffsets.add(i);
                }

                Socket neighbor = null;
                int virtualNodeIdxOffset = 0,virtualNodeNeighborIdx = 0;

                while (!availableOffsets.isEmpty()) {
                    int virtualNodeIdxOffsetIndex = ThreadLocalRandom.current().nextInt(availableOffsets.size());
                    virtualNodeIdxOffset = availableOffsets.get(virtualNodeIdxOffsetIndex);

                    virtualNodeNeighborIdx = Math.floorMod(virtualNodeIdx + virtualNodeIdxOffset, nrNodes);
                    neighbor = server.consistentHashing.getNthNodeSocket(virtualNodeNeighborIdx);
                    TokenNode node = server.consistentHashing.getNode(virtualNodeNeighborIdx);

                    if (neighbor != null && node.isActive()) {
                        break;  // Exit the loop if a socket is retrieved
                    } else {
                        // Remove the used offset from the list
                        availableOffsets.remove(virtualNodeIdxOffsetIndex);
                    }
                }

                if(neighbor != null) {
                    int startingHashIdx;
                    int endingHashIdx;

                    if (virtualNodeIdxOffset < 0) {
                        startingHashIdx = Math.floorMod(virtualNodeNeighborIdx - nrReplicas - virtualNodeIdxOffset, nrNodes);
                        endingHashIdx = virtualNodeNeighborIdx;
                    } else {
                        startingHashIdx = Math.floorMod(virtualNodeIdx - nrReplicas + virtualNodeIdxOffset, nrNodes);
                        endingHashIdx = virtualNodeIdx;
                    }

                    String startingHash = server.consistentHashing.getNthNodeHash(startingHashIdx);
                    String endingHash = server.consistentHashing.getNthNodeHash(endingHashIdx);

                    Map<String, String> shoppingListsBase64 = server.getDB().getShoppingListsBase64(startingHash, endingHash);
                    StringBuilder synchronizationMessageBuilder = new StringBuilder("SYNCHRONIZE" + " " + startingHash + " " + endingHash + " ");

                    for (String shoppingListID : shoppingListsBase64.keySet()) {
                        String shoppingList = shoppingListsBase64.get(shoppingListID);
                        String shoppingListHash = Hasher.md5(shoppingList);

                        synchronizationMessageBuilder.append(shoppingListID).append(":").append(shoppingListHash).append(',');
                    }

                    if (!synchronizationMessageBuilder.isEmpty()) {
                        synchronizationMessageBuilder.setLength(synchronizationMessageBuilder.length() - 1);
                    }

                    String synchronizationMessage = synchronizationMessageBuilder.toString();
                    synchronized (writeQueue) {
                        writeQueue.add(new Message(synchronizationMessage, neighbor));
                    }
                }

            }
            try {
                Thread.sleep(nrSecondsBetweenSynchronization*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
