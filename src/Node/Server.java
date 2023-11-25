package Node;


import Node.ConsistentHashing.ConsistentHashing;
import Node.Gossiper.Gossiper;
import NioChannels.Message.MessageProcessorBuilder;
import NioChannels.Socket.Socket;
import Utils.Hasher;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;

import java.util.*;


public abstract class Server extends NioChannels.Server
{
    protected String nodeId;
    public final ConsistentHashing consistentHashing;
    public final Gossiper gossiper;
    private Thread gossiperThread;

    public Server(String confFilePath, int port,int nrReplicas, MessageProcessorBuilder messageProcessorBuilder ) throws IOException {
        super(port, messageProcessorBuilder);

        this.nodeId = UUID.randomUUID().toString();
        this.consistentHashing = new ConsistentHashing(nrReplicas);
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
                connect(currentNeighborAddress);

            }
            myReader.close();
        } catch (FileNotFoundException e) {

        }
    }

    public Socket connect(InetSocketAddress inetSocketAddress) {
        Socket socket = super.connect(inetSocketAddress);
        if (socket != null)
            gossiper.addNeighbor(socket);
        return socket;
    }

    public String getNodeId() {
        return  nodeId;
    }

    public boolean knowsAboutRingNode(String nodeID) throws NoSuchAlgorithmException {
        String nodeIDHash = Hasher.md5(nodeID);
        return consistentHashing.getHashToNode().containsKey(nodeIDHash) || nodeID.equals(this.nodeId);
    }

    public void addLBNode(Socket socket,String socketID){
        gossiper.addNeighbor(socket);
        gossiper.addNeighborID(socketID);
    }

    public boolean knowsAboutLBNode(String socketID){
        return gossiper.getNeighborIDs().contains(socketID);
    }

}