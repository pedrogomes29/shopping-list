package Node;

import Node.ConsistentHashing.ConsistentHashing;
import Node.ConsistentHashing.TokenNode;
import Node.Gossiper.Gossiper;
import NioChannels.Message.MessageProcessorBuilder;
import NioChannels.Socket.Socket;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.*;


public abstract class Server extends NioChannels.Server  {
    protected String nodeId;
    public final ConsistentHashing consistentHashing;
    public final Gossiper gossiper;
    private Thread gossiperThread;

    private final int nrVirtualNodesPerNode;

    public Server(String confFilePath, int port,int nrReplicas,int nrVirtualNodesPerNode, MessageProcessorBuilder messageProcessorBuilder ) throws IOException {
        super(port, messageProcessorBuilder);

        this.nodeId = UUID.randomUUID().toString();
        this.consistentHashing = new ConsistentHashing(nrReplicas,nrVirtualNodesPerNode);
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
                String[] lineParts = line.split(":");

                String host = lineParts[0];
                int port = Integer.parseInt(lineParts[1]);

                InetSocketAddress currentNeighborAddress = new InetSocketAddress(host,port);
                connect(currentNeighborAddress);

            }
            myReader.close();
        } catch (FileNotFoundException e) {

        }
    }

    public Socket connectWithoutAddingNeighbor(String host, int port) {
        System.out.println("Hello5");
        return connectWithoutAddingNeighbor(new InetSocketAddress(host, port));
    }

    public Socket connectWithoutAddingNeighbor(InetSocketAddress inetSocketAddress) {
        System.out.println("Hello6");
        return super.connect(inetSocketAddress);
    }

    public Socket connect(String host, int port){
        return connect(new InetSocketAddress(host, port));
    }

    public Socket connect(InetSocketAddress inetSocketAddress) {
        System.out.println("Hello1");
        Socket socket = super.connect(inetSocketAddress);
        System.out.println("Hello2");
        if (socket != null)
            gossiper.addNeighbor(socket);
        return socket;
    }

    public String getNodeId() {
        return  nodeId;
    }

    public boolean knowsAboutRingNode(String nodeID) throws NoSuchAlgorithmException {
        for(String virtualNodeIDHash:TokenNode.getVirtualNodesHashes(nodeID,nrVirtualNodesPerNode)){
            if(consistentHashing.getHashToNode().containsKey(virtualNodeIDHash))
                return true;
        }
        return false;
    }

    public void addLBNode(Socket socket, String socketID) {
        gossiper.addNeighbor(socket);
        gossiper.addNeighborID(socketID);
    }

    public boolean knowsAboutLBNode(String socketID) {
        return gossiper.getNeighborIDs().contains(socketID);
    }

    public void addAdminNode(Socket socket, String socketID) {
        gossiper.addNeighbor(socket);
        gossiper.addNeighborID(socketID);
    }

    public boolean knowsAboutAdminNode(String socketID) {
        return gossiper.getNeighborIDs().contains(socketID);
    }

    public boolean alreadyRemovedNode(String nodeID) {
        return !consistentHashing.containsNode(nodeID);
    }
}