package RingNode.ConsistentHashing;

import Node.ConsistentHashing.TokenNode;
import RingNode.Server;

import java.util.*;

public class ConsistentHashing extends Node.ConsistentHashing.ConsistentHashing{
    private Set<String> myVirtualNodeHashes;
    private Server server;

    public ConsistentHashing(int nrReplicas, int nrVirtualNodesPerNode, Server server) {
        super(nrReplicas,nrVirtualNodesPerNode);
        this.myVirtualNodeHashes = new HashSet<>();
        this.server = server;
    }


    public synchronized boolean addNodeToRing(TokenNode node) {
        String[] virtualNodeHashes = TokenNode.getVirtualNodesHashes(node.getId(),nrVirtualNodesPerNode);
        int nrRealNodes = getNrRealNodes();
        for(String virtualNodeHash:virtualNodeHashes) {
            if (hashToNode.containsKey(virtualNodeHash))
                return false;
            int positionToInsert = binarySearch(virtualNodeHash);
            if(nrRealNodes<nrReplicas)
                continue;

            for (int startingNodeOffset = -1; startingNodeOffset >= -nrReplicas; startingNodeOffset--) {
                Set<TokenNode> realNodes = new HashSet<>();
                TokenNode nodeToDelete = null;

                for (int nodeToDeleteOffset = 1; nodeToDeleteOffset < getNrNodes(); nodeToDeleteOffset++) {
                    int nodeToDeleteIdx = Math.floorMod(positionToInsert + startingNodeOffset + nodeToDeleteOffset,getNrNodes());
                    nodeToDelete = getNthNode(nodeToDeleteIdx);
                    if (!realNodes.contains(nodeToDelete)) {
                        if (realNodes.size() == nrReplicas - 1)
                            break;
                        else
                            realNodes.add(nodeToDelete);
                    }
                }

                if(!Objects.equals(nodeToDelete.getId(), server.getNodeId())) //if the server node isnt the one which
                    continue;                                                 //has to delete the keys, it doesnt need to do anything


                String startingHashToRemove = null;
                int endingHashIdx = Math.floorMod(positionToInsert + startingNodeOffset + 1, getNrNodes());
                String endingHashToRemove;
                if(endingHashIdx==positionToInsert)
                    endingHashToRemove = virtualNodeHash;
                else
                    endingHashToRemove = getNthNodeHash(endingHashIdx);

                for (int startingHashOffset = 0; startingHashOffset < getNrNodes(); startingHashOffset++) {
                    int startingHashIdx = Math.floorMod(positionToInsert + startingNodeOffset - startingHashOffset,getNrNodes());
                    TokenNode startingHashNode = getNthNode(startingHashIdx);
                    if (!realNodes.contains(startingHashNode)) {
                        startingHashToRemove = getNthNodeHash(startingHashIdx);
                        break;
                    }
                }
                System.out.println("Deleted shopping lists from hashes" + " " +
                        startingHashToRemove + " to " + endingHashToRemove);
                server.getDB().deleteShoppingLists(startingHashToRemove, endingHashToRemove);
            }

        }
        for(String virtualNodeHash:virtualNodeHashes) {
            int positionToInsert = binarySearch(virtualNodeHash);
            hashToNode.put(virtualNodeHash, node);
            nodeHashes.add(positionToInsert, virtualNodeHash);
        }

        return true;
    }

    public synchronized boolean addSelfToRing(TokenNode self) {
        String[] virtualNodeHashes = TokenNode.getVirtualNodesHashes(self.getId(),nrVirtualNodesPerNode);
        for(String virtualNodeHash:virtualNodeHashes) {
            if (hashToNode.containsKey(virtualNodeHash))
                return false;


            hashToNode.put(virtualNodeHash, self);
            int positionToInsert = binarySearch(virtualNodeHash);
            nodeHashes.add(positionToInsert, virtualNodeHash);
            myVirtualNodeHashes.add(virtualNodeHash);
        }

        return true;
    }
}
