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
import java.security.NoSuchAlgorithmException;

import java.util.*;


public abstract class Server extends NioChannels.Server
{
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
        try {
            File myObj = new File(confFilePath);
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String line = myReader.nextLine();
                String[] lineParts = line.split(":");

                String host = lineParts[0];
                int port = Integer.parseInt(lineParts[1]);

                InetSocketAddress currentNeighborAddress = new InetSocketAddress(host,port);
                Socket currentNeighborSocket = connect(currentNeighborAddress);
                Queue<Message> writeQueue = getWriteQueue();
                synchronized (writeQueue){
                    String messageToStartRumour = "RUMOUR" + " ";
                    if (this instanceof LoadBalancer.Server)
                        messageToStartRumour += "ADD_LB" + " ";
                    else if(this instanceof RingNode.Server)
                        messageToStartRumour += "ADD_NODE" + " ";

                    messageToStartRumour +=  this.nodeId + " " + this.port;

                    writeQueue.add(new Message(messageToStartRumour,currentNeighborSocket));
                }
            }

        }catch (FileNotFoundException e) {
            System.err.println("Warning: Nighbors Conf " + confFilePath + " not found");
        }

    }


    public String getNodeId() {
        return  nodeId;
    }

    public boolean knowsAboutRingNode(String nodeID) {
        for(String virtualNodeIDHash:TokenNode.getVirtualNodesHashes(nodeID,nrVirtualNodesPerNode)){
            if(consistentHashing.containsHash(virtualNodeIDHash))
                return true;
        }
        return false;
    }

    public boolean knowsAboutLBNode(String nodeID){
        return gossiper.getNeighbors().containsKey(nodeID) || nodeID.equals(nodeId);
    }

    @Override
    public void removeSocket(Socket socket) {
        super.removeSocket(socket);
        gossiper.removeNeightbor(socket);

        consistentHashing.markTemporaryNode(socket);
    }
}