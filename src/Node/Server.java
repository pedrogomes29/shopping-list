package Node;


import Node.ConsistentHashing.TokenNode;
import Node.ConsistentHashing.ConsistentHashing;
import Node.Gossiper.Gossiper;
import Node.Message.Message;
import Node.Message.MessageProcessorBuilder;
import Node.Socket.Socket;
import Node.Socket.SocketAccepter;
import Node.Socket.SocketProcessor;
import Utils.Hasher;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;


public abstract class Server
{
    public final int port;
    protected String nodeId;
    public boolean running = true;

    protected final Map<Long, Socket> socketMap;
    private final SocketProcessor socketProcessor;
    private final SocketAccepter socketAccepter;

    // Threads
    private Thread accepterThread;
    private Thread processorThread;
    private Thread gossiperThread;

    // Queues
    protected final Queue<Message> outboundMessageQueue;
    protected final Queue<Socket> socketQueue;
    protected final Queue<Socket> connectionQueue;

    public final ConsistentHashing consistentHashing;
    public final Gossiper gossiper;

    public Queue<Message> getWriteQueue(){
        return outboundMessageQueue;
    }


    public Server(String confFilePath, int port,int nrReplicas, MessageProcessorBuilder messageProcessorBuilder ) throws IOException {
        this.port = port;
        messageProcessorBuilder.setServer(this);
        this.outboundMessageQueue = new LinkedList<>();
        this.socketQueue = new ArrayBlockingQueue<>(1024);
        this.socketMap = new ConcurrentHashMap<>();

        this.nodeId = UUID.randomUUID().toString();
        this.consistentHashing = new ConsistentHashing(nrReplicas);
        this.connectionQueue = new ConcurrentLinkedQueue<>();

        socketProcessor = new SocketProcessor(this,socketQueue, messageProcessorBuilder,
                this.outboundMessageQueue, this.socketMap, this.connectionQueue);
        socketAccepter = new SocketAccepter(this, socketQueue);

        gossiper = new Gossiper(this, this.outboundMessageQueue);

        connectToNeighborsFromConf(confFilePath);
    }




    public void stopServer() throws InterruptedException
    {
        running = false;
        accepterThread.interrupt();
        accepterThread.join();
        processorThread.join();
        gossiperThread.interrupt();
        gossiperThread.join();

    }

    public void startThreads()
    {
        accepterThread = new Thread(socketAccepter);
        processorThread = new Thread(socketProcessor);
        gossiperThread = new Thread(gossiper);

        accepterThread.start();
        processorThread.start();
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

    public Socket connect(String host, int port){
        return connect(new InetSocketAddress(host, port));
    }

    public Socket connect(InetSocketAddress inetSocketAddress) {
        try {
            System.out.println("Connect :" + inetSocketAddress.toString());
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            socketChannel.connect(inetSocketAddress);

            Socket socket = new Socket(socketChannel);

            connectionQueue.offer(socket);

            gossiper.addNeighbor(socket);

            return socket;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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

    public Map<Long, Socket> getSocketMap(){
        return socketMap;
    }
}