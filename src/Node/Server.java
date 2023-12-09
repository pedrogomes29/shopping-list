package Node;

import NioChannels.Message.Message;
import Node.ConsistentHashing.ConsistentHashing;
import Node.ConsistentHashing.TokenNode;
import Node.Gossiper.Gossiper;
import NioChannels.Message.MessageProcessorBuilder;
import NioChannels.Socket.Socket;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

public abstract class Server extends NioChannels.Server  {
    protected String nodeId;
    public ConsistentHashing consistentHashing;
    public final Gossiper gossiper;
    private Thread gossiperThread;

    private final int nrVirtualNodesPerNode;

    public Server(String nodeId, String confFilePath, int port,int nrReplicas,int nrVirtualNodesPerNode, MessageProcessorBuilder messageProcessorBuilder ) throws IOException {
        super(port, messageProcessorBuilder);

        this.nodeId = nodeId;
        System.out.println("id: "+ nodeId);
        this.nrVirtualNodesPerNode = nrVirtualNodesPerNode;
        gossiper = new Gossiper(this, this.outboundMessageQueue);

        connectToNeighborsFromConf(confFilePath);
    }

    public ConsistentHashing getConsistentHashing() {
        return this.consistentHashing;
    }

    public void stopServer() throws InterruptedException {
        super.stopServer();
        gossiperThread.interrupt();
        gossiperThread.join();
    }

    public void startThreads() {
        super.startThreads();
        gossiperThread = new Thread(gossiper);
        gossiperThread.start();
    }

    private void connectToNeighborsFromConf(String confFilePath) {
        try {
            File myObj = new File(confFilePath);
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String line = myReader.nextLine();

                if (line.isBlank())
                    continue;

                String[] lineParts = line.split(":");

                String host = lineParts[0];
                int port = Integer.parseInt(lineParts[1]);

                InetSocketAddress currentNeighborAddress = new InetSocketAddress(host,port);
                Socket currentNeighborSocket = connect(currentNeighborAddress);
                Queue<Message> writeQueue = getWriteQueue();
                synchronized (writeQueue) {
                    String messageToStartRumour = "RUMOUR ";
                    if (this instanceof LoadBalancer.Server)
                        messageToStartRumour += "ADD_LB ";
                    else if (this instanceof RingNode.Server)
                        messageToStartRumour += "ADD_NODE ";
                    else if (this instanceof Admin.Server)
                        messageToStartRumour += "ADD_ADMIN ";
                    messageToStartRumour +=  this.nodeId + " " + this.port;

                    writeQueue.add(new Message(messageToStartRumour,currentNeighborSocket));
                }
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.err.println("Warning: Error reading conf file!");
        }
    }

    public String getNodeId() {
        return  nodeId;
    }

    public boolean knowsAboutRingNode(String nodeID)  {
        for (String virtualNodeIDHash:TokenNode.getVirtualNodesHashes(nodeID,nrVirtualNodesPerNode)) {
            TokenNode node = consistentHashing.getHashToNode().get(virtualNodeIDHash);
            if (node != null && node.isActive())
                return true;
        }
        return false;
    }

    public boolean knowsAboutLBNode(String nodeID) {
        return gossiper.getNeighbors().containsKey(nodeID) || nodeID.equals(nodeId);
    }

    public boolean knowsAboutAdminNode(String nodeID) {
        return gossiper.getNeighbors().containsKey(nodeID) || nodeID.equals(nodeId);
    }

    public boolean alreadyRemovedNode(String nodeID) {
        for (TokenNode tokenNode: consistentHashing.getHashToNode().values()) {
            if (Objects.equals(tokenNode.getId(), nodeID))
                return false;
        }
        return true;
    }
    
    @Override
    public void removeSocket(Socket socket) {
        super.removeSocket(socket);
        gossiper.removeNeighbor(socket);

        consistentHashing.markTemporaryNode(socket);
    }
}