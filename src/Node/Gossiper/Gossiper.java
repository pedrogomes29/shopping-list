package Node.Gossiper;

import NioChannels.Message.Message;
import NioChannels.Socket.Socket;
import Node.ConsistentHashing.ConsistentHashing;
import Node.ConsistentHashing.TokenNode;
import Node.Server;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import Node.Node;

public class Gossiper implements Runnable{

    private static final int rumour_count = 3;
    private static final int nrNeighborsToGossipTo = 2;

    private static final int nrSecondsBetweenGossipRounds = 1;

    private static final int nrSecondsBetweenTopologySync = 20;

    private static final int lcm = lcm(nrSecondsBetweenTopologySync,nrSecondsBetweenGossipRounds);

    private final Map<String,Node> neighbors;
    private final Map<String,Integer> rumours;
    private final Queue<Message> writeQueue;
    private final Server server;


    private static int gcd(int a, int b) {
        while (b != 0) {
            int temp = b;
            b = a % b;
            a = temp;
        }
        return a;
    }

    private static int lcm(int a, int b) {
        return (a * b) / gcd(a, b);
    }

    public Gossiper(Server server, Queue<Message> writeQueue){
        this.neighbors = new HashMap<>();
        this.rumours = new HashMap<>();
        this.writeQueue = writeQueue;
        this.server = server;
    }

    private void syncTopology(String neighborToSyncWithID){
        StringBuilder syncMessageBuilder = new StringBuilder("SYNC_TOPOLOGY" + " " + server.getNodeId() + " ");
        for(String neighborID:neighbors.keySet()){
            if(!neighborID.equals(neighborToSyncWithID) && !neighborID.equals(server.getNodeId())){
                Node neighborToSyncWith = neighbors.get(neighborID);


                InetSocketAddress neighborToSyncWithEndpoint = neighborToSyncWith.getNodeEndpoint();
                if(neighborToSyncWith instanceof TokenNode){
                    syncMessageBuilder.append("NODE").append(' ');
                }
                else {
                    syncMessageBuilder.append("LB").append(' ');
                }

                syncMessageBuilder.append(neighborID).append(' ').
                append(neighborToSyncWithEndpoint.getHostString()).append(':')
                .append(neighborToSyncWithEndpoint.getPort()).append(',');
            }
        }
        if (syncMessageBuilder.toString().endsWith(",")) {
            syncMessageBuilder.setLength(syncMessageBuilder.length() - 1);
        }
        String syncMessage = syncMessageBuilder.toString();

        synchronized (writeQueue) {
            writeQueue.add(new Message(syncMessage, neighbors.get(neighborToSyncWithID).getSocket()));
        }
    }

    @Override
    public void run() {
        short secondsPassed=0;
        while(server.running) {
            System.out.println("Nr Neighbors: " + neighbors.size());

            if(secondsPassed%lcm==0)
                secondsPassed = 0;
            if(secondsPassed%nrSecondsBetweenGossipRounds==0) {
                ArrayList<String> neighborIDs;
                synchronized (neighbors) {
                    neighborIDs = new ArrayList<>(neighbors.keySet());
                }

                for (int i = 0; i < nrNeighborsToGossipTo && !neighborIDs.isEmpty(); i++) {
                    int neighborToGossipToIdx = ThreadLocalRandom.current().nextInt(0, neighborIDs.size());
                    String neighborToGossipToID = neighborIDs.get(neighborToGossipToIdx);

                    Socket socket = neighbors.get(neighborToGossipToID).getSocket();
                    if( !socket.socketChannel.isConnected()){
                        i--;
                        continue;
                    }

                    for (String rumour : rumours.keySet()) {
                        synchronized (writeQueue) {
                            writeQueue.add(new Message("RUMOUR" + " " + rumour, neighbors.get(neighborToGossipToID).getSocket()));
                        }
                    }

                    neighborIDs.remove(neighborToGossipToIdx);
                }
            }

            if(secondsPassed%nrSecondsBetweenTopologySync==0) {
                ArrayList<String> neighborIDs;
                synchronized (neighbors) {
                    neighborIDs = new ArrayList<>(neighbors.keySet());
                }

                for (int i = 0; i < nrNeighborsToGossipTo && !neighborIDs.isEmpty(); i++) {
                    int neighborToGossipToIdx = ThreadLocalRandom.current().nextInt(0, neighborIDs.size());
                    String neighborToGossipToID = neighborIDs.get(neighborToGossipToIdx);
                    syncTopology(neighborToGossipToID);
                    neighborIDs.remove(neighborToGossipToIdx);
                }
            }

            secondsPassed++;
            try {
                Thread.sleep(1000);
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

    public void removeNeighbor(Socket socket) {
        synchronized (neighbors) {
            Iterator<Map.Entry<String, Node>> iterator = neighbors.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, Node> entry = iterator.next();
                Node node = entry.getValue();

                if (node.getSocket().equals(socket)) {
                    iterator.remove();
                }
            }
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
