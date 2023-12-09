package RingNode.Synchronizer;

import NioChannels.Message.Message;
import Node.ConsistentHashing.TokenNode;
import RingNode.Server;
import NioChannels.Socket.Socket;

import java.util.*;

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


    public void sendSynchronize(String startingHash, String endingHash, Socket neighbor){
        Map<String, String> shoppingListsHashes = server.getDB().getShoppingListsHashes(startingHash, endingHash);
        StringBuilder synchronizationMessageBuilder = new StringBuilder("SYNCHRONIZE" + " " + startingHash + " " + endingHash + " ");

        for (String shoppingListID : shoppingListsHashes.keySet()) {
            String shoppingListHash = shoppingListsHashes.get(shoppingListID);
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

    public void sync(String virtualNodeHash){
        int virtualNodeIdx = server.consistentHashing.binarySearch(virtualNodeHash);
        TokenNode self = server.consistentHashing.getNthNode(virtualNodeIdx);
        Map<TokenNode,String> realNodesToEndingHash = new HashMap<>();
        String startingHash = null;
        for(int offsetIdx=1;offsetIdx<server.consistentHashing.getNrNodes();offsetIdx++){
            int otherVirtualNodeIdx = Math.floorMod(virtualNodeIdx - offsetIdx,server.consistentHashing.getNrNodes());
            TokenNode otherVirtualNode = server.consistentHashing.getNthNode(otherVirtualNodeIdx);

            if(otherVirtualNode.equals(self)) {
                startingHash = server.consistentHashing.getNthNodeHash(otherVirtualNodeIdx);
                break;
            }
            if(!realNodesToEndingHash.containsKey(otherVirtualNode)){
                if(realNodesToEndingHash.size()==nrReplicas-1){
                    startingHash = server.consistentHashing.getNthNodeHash(otherVirtualNodeIdx);
                    break;
                }
                else {
                    String otherVirtualNodeEndingHash = server.consistentHashing.getNthNodeHash(otherVirtualNodeIdx);
                    realNodesToEndingHash.put(otherVirtualNode, otherVirtualNodeEndingHash);
                }
            }
        }

        for(TokenNode nodeToSyncWith:realNodesToEndingHash.keySet()){
            sendSynchronize(startingHash, realNodesToEndingHash.get(nodeToSyncWith),nodeToSyncWith.getSocket());
        }
    }

    @Override
    public void run() {
        while(server.running) {
            synchronized(server.consistentHashing) {
                int nrRealNodes = server.consistentHashing.getNrRealNodes();

                if (nrRealNodes >= nrReplicas) {
                    String[] virtualNodeHashes;

                    virtualNodeHashes = TokenNode.getVirtualNodesHashes(server.getNodeId(), nrVirtualNodesPerNode);


                    for (String virtualNodeHash : virtualNodeHashes) {
                        sync(virtualNodeHash);
                    }
                }
            }
            try {
                Thread.sleep(nrSecondsBetweenSynchronization * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
