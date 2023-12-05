package Node.Gossiper;

import NioChannels.Message.Message;
import Node.Server;
import NioChannels.Socket.Socket;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Gossiper implements Runnable{

    private static final int rumour_count = 3;
    private static final int nrNeighborsToGossipTo = 2;

    private static final int nrSecondsBetweenGossipRounds = 1;

    private final Set<Socket> neighbors;
    private final Map<String,Integer> rumours;
    private final Queue<Message> writeQueue;
    private final Server server;
    protected final Set<String> neighborIDs;


    public Gossiper(Server server, Queue<Message> writeQueue){
        this.neighbors = new HashSet<>();
        this.neighborIDs = new HashSet<>();
        this.rumours = new HashMap<>();
        this.writeQueue = writeQueue;
        this.server = server;
    }

    @Override
    public void run() {
        while(server.running) {
            System.out.println("Nr Neighbors: " + neighbors.size());
            ArrayList<Socket> neighborsCopy;
            synchronized (neighbors){
                neighborsCopy = new ArrayList<>(neighbors);
            }

            for (int i = 0; i < nrNeighborsToGossipTo && !neighborsCopy.isEmpty(); i++) {
                int neighborToGossipToIdx = ThreadLocalRandom.current().nextInt(0, neighborsCopy.size());
                Socket neighborToGossipTo = neighborsCopy.get(neighborToGossipToIdx);
                if (neighborToGossipTo.socketChannel.isConnected()) {
                    for (String rumour : rumours.keySet()) {
                        synchronized (writeQueue) {
                            writeQueue.add(new Message("RUMOUR" + " " + rumour, neighborToGossipTo));
                        }
                    }
                }
                neighborsCopy.remove(neighborToGossipToIdx);
            }

            try {
                Thread.sleep(nrSecondsBetweenGossipRounds* 1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void addNeighbor(Socket neighbor){
        synchronized (this.neighbors) {
            neighbors.add(neighbor);
        }
    }

    public void addRumour(String newRumour){
        rumours.put(newRumour,rumour_count);
    }

    public Map<String, Integer> getRumours(){
        return rumours;
    }

    public Set<Socket> getNeighbors() {
        return neighbors;
    }

    public Set<String> getNeighborIDs() {
        return neighborIDs;
    }

    public void addNeighborID(String nodeId) {
        synchronized (neighborIDs) {
            neighborIDs.add(nodeId);
        }
    }

    public void removeNeightbor(Socket socket) {
        synchronized (neighborIDs) {
            neighbors.remove(socket);
        }
    }
}
