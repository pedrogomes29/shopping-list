package Node.Gossiper;

import Node.Message.Message;
import Node.Server;
import Node.Socket.Socket;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Gossiper implements Runnable{

    private static int nrNeighborsToGossipTo = 2;

    private static int nrSecondsBetweenGossipRounds = 1;

    private final Set<Socket> neighbors;
    private Map<String,Integer> rumours;
    private final Queue<Message> writeQueue;
    private final Server server;


    public Gossiper(Server server, Set<Socket> neighbors, Map<String,Integer> rumours, Queue<Message> writeQueue){
        this.neighbors = neighbors;
        this.rumours = rumours;
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
                for (String rumour : rumours.keySet()) {
                    synchronized (writeQueue) {
                        writeQueue.add(new Message("RUMOUR" + " " + rumour, neighborToGossipTo));
                    }
                }
                neighborsCopy.remove(neighborToGossipToIdx);
            }

            try {
                Thread.sleep(nrSecondsBetweenGossipRounds*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
