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

import java.util.*;


public abstract class Server extends NioChannels.Server
{
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

    public void stopServer() throws InterruptedException
    {
        super.stopServer();
        gossiperThread.interrupt();
        gossiperThread.join();

    }

    public void startThreads()
    {
        super.startThreads();
        gossiperThread = new Thread(gossiper);
        gossiperThread.start();

    }

    private void connectToNeighborsFromConf(String confFilePath){
        if (confFilePath == null){
            System.err.println("Warning: Nighbors Conf not provided");
            return;
        }

        try (Scanner myReader = new Scanner(new File(confFilePath))){

            while (myReader.hasNextLine()) {
                String line = myReader.nextLine();
                String[] lineParts = line.split(" ");

                String neighborID = lineParts[0];
                String neighborEndpoint = lineParts[1];
                String[] neighborEndpointParts = neighborEndpoint.split(":");

                String neighborHost = neighborEndpointParts[0];
                int neighborPort = Integer.parseInt(neighborEndpointParts[1]);

                InetSocketAddress currentNeighborAddress = new InetSocketAddress(neighborHost,neighborPort);
                Socket socket = this.connect(currentNeighborAddress);
                gossiper.addNeighbor(new Node(socket,neighborID,currentNeighborAddress));
            }

            myReader.close();
        }catch (FileNotFoundException e) {
            System.err.println("Warning: Nighbors Conf " + confFilePath + " not found");
        }
    }


    public String getNodeId() {
        return  nodeId;
    }

    public boolean knowsAboutRingNode(String nodeID) {
        for(String virtualNodeIDHash:TokenNode.getVirtualNodesHashes(nodeID,nrVirtualNodesPerNode)){
            if(consistentHashing.getHashToNode().containsKey(virtualNodeIDHash))
                return true;
        }
        return false;
    }

    public void addLBNode(Node lbNode){
        gossiper.addNeighbor(lbNode);
    }

    public boolean knowsAboutLBNode(String socketID){
        return gossiper.getNeighbors().containsKey(socketID);
    }

    @Override
    public void removeSocket(Socket socket) {
        super.removeSocket(socket);
        gossiper.removeNeightbor(socket);
    }
}