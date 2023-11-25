package NIOChannels;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Gossiper implements Runnable{

    private static int nrNeighborsToGossipTo = 2;

    private static int nrSecondsBetweenGossipRounds = 1;

    private ArrayList<Socket> neighbors;
    private Map<String,Integer> rumours;
    private final Queue<Message> writeQueue;
    private final Server server;


    public Gossiper(Server server, ArrayList<Socket> neighbors, Map<String,Integer> rumours, Queue<Message> writeQueue){
        this.neighbors = neighbors;
        this.rumours = rumours;
        this.writeQueue = writeQueue;
        this.server = server;
    }

    @Override
    public void run() {
        while(server.running) {
            ArrayList<Socket> neighborsCopy = new ArrayList<>(neighbors);

            for (int i = 0; i < nrNeighborsToGossipTo && !neighborsCopy.isEmpty(); i++) {
                int neighborToGossipToIdx = ThreadLocalRandom.current().nextInt(0, neighborsCopy.size());
                Socket neighborToGossipTo = neighbors.get(neighborToGossipToIdx);
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
