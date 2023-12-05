package Node.Gossiper;

import NioChannels.Message.Message;
import Node.Server;
import NioChannels.Socket.Socket;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import Node.Node;

public class Gossiper implements Runnable{

    private static final int rumour_count = 3;
    private static int nrNeighborsToGossipTo = 2;

    private static int nrSecondsBetweenGossipRounds = 1;

    private final Map<String,Node> neighbors;
    private final Map<String,Integer> rumours;
    private final Queue<Message> writeQueue;
    private final Server server;


    public Gossiper(Server server, Queue<Message> writeQueue){
        this.neighbors = new HashMap<>();
        this.rumours = new HashMap<>();
        this.writeQueue = writeQueue;
        this.server = server;
    }

    private void syncTopology(String neighborToSyncWithID){
        StringBuilder syncMessageBuilder = new StringBuilder("SYNC_TOPOLOGY ");
        for(String neighborID:neighbors.keySet()){
            if(neighborID.equals(neighborToSyncWithID)){
                Node neighborToSyncWith = neighbors.get(neighborID);
                InetSocketAddress neighborToSyncWithEndpoint = neighborToSyncWith.getNodeEndpoint();
                syncMessageBuilder.append(neighborID).append(' ')
                .append(neighborToSyncWithEndpoint.getHostString()).append(':')
                .append(neighborToSyncWithEndpoint.getPort()).append(',');
            }
        }
        if (syncMessageBuilder.toString().endsWith(",")) {
            syncMessageBuilder.setLength(syncMessageBuilder.length() - 1);
            String syncMessage = syncMessageBuilder.toString();
            synchronized (writeQueue) {
                writeQueue.add(new Message(syncMessage, neighbors.get(neighborToSyncWithID).getSocket()));
            }
        }
    }

    @Override
    public void run() {
        while(server.running) {
            System.out.println("Nr Neighbors: " + neighbors.size());
            ArrayList<String> neighborIDs;
            synchronized (neighbors){
                neighborIDs = new ArrayList<>(neighbors.keySet());
            }

            for (int i = 0; i < nrNeighborsToGossipTo && !neighborIDs.isEmpty(); i++) {
                int neighborToGossipToIdx = ThreadLocalRandom.current().nextInt(0, neighborIDs.size());
                String neighborToGossipToID = neighborIDs.get(neighborToGossipToIdx);
                if(neighbors.get(neighborToGossipToID).getSocket()==null){ //neighbor is itself, skip
                    i--;
                    continue;
                }
                for (String rumour : rumours.keySet()) {
                    synchronized (writeQueue) {
                        writeQueue.add(new Message("RUMOUR" + " " + rumour, neighbors.get(neighborToGossipToID).getSocket()));
                    }
                }

                syncTopology(neighborToGossipToID);


                neighborIDs.remove(neighborToGossipToIdx);
            }

            try {
                Thread.sleep(nrSecondsBetweenGossipRounds*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void addNeighbor(Node neighbor){
        synchronized (this.neighbors) {
            neighbors.put(neighbor.getId(),neighbor);
        }
    }

    public void addRumour(String newRumour){
        rumours.put(newRumour,rumour_count);
    }

    public Map<String, Integer> getRumours(){
        return rumours;
    }

    public Map<String,Node> getNeighbors() {
        return neighbors;
    }


}
