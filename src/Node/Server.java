package Node;


import LoadBalancer.TokenNode;
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;


public abstract class Server
{
    public final int port;

    public boolean running = true;

    private Thread accepterThread;
    private Thread processorThread;

    private Thread gossiperThread;

    static final int rumour_count = 3;
    protected final Map<String,Integer> rumours;
    protected final Queue<Message> outboundMessageQueue;

    protected final Queue<Socket> socketQueue;

    protected final Queue<Socket> connectionQueue;


    protected final Map<Long, Socket> socketMap;

    private final ArrayList<String> nodeHashes;
    private final HashMap<String,TokenNode> hashToNode;

    protected String nodeId;


    private final SocketProcessor socketProcessor;
    private final SocketAccepter socketAccepter;
    private final Gossiper gossiper;
    private final Set<Socket> neighbors;

    public Queue<Message> getWriteQueue(){
        return outboundMessageQueue;
    }

    public Server(int port, MessageProcessorBuilder messageProcessorBuilder ) throws IOException {
        this.port = port;
        messageProcessorBuilder.setServer(this);
        this.outboundMessageQueue = new LinkedList<>();
        this.socketQueue = new ArrayBlockingQueue<>(1024);
        this.socketMap = new HashMap<>();
        this.neighbors = new HashSet<>();
        this.nodeId = UUID.randomUUID().toString();
        this.rumours = new HashMap<>();
        this.nodeHashes = new ArrayList<>();
        this.hashToNode = new HashMap<>();
        this.connectionQueue = new ConcurrentLinkedQueue<>();

        socketProcessor = new SocketProcessor(this,socketQueue, messageProcessorBuilder,
                this.outboundMessageQueue, this.socketMap, this.connectionQueue);
        socketAccepter = new SocketAccepter(this, socketQueue);

        gossiper = new Gossiper(this,this.neighbors,this.rumours,this.outboundMessageQueue);

        nodeHashes.add(nodeId);

        addRumour("ADD_NODE " + nodeId + " " + port );
        connectToNeighborsFromConf("conf/conf.txt");
    }

    public synchronized void addNodeToRing(TokenNode nodeId) throws NoSuchAlgorithmException {
        String nodeIdHash = nodeId.getHash();
        if (hashToNode.containsKey(nodeIdHash) )
            return;

        synchronized (this.neighbors) {
            neighbors.add(nodeId.getSocket());
        }

        hashToNode.put(nodeIdHash,nodeId);
        int positionToInsert = binarySearch(nodeIdHash);

        nodeHashes.add(positionToInsert,nodeIdHash);
    }

    public TokenNode getNode(String nodeId) throws NoSuchAlgorithmException {
        String nodeIdHash = Hasher.md5(nodeId);

        return hashToNode.get(nodeIdHash);
    }

    private int binarySearch(String hash) {
        int low = 0;
        int high = nodeHashes.size() - 1;

        while (low <= high) {
            int mid = (low + high) / 2;
            String midVal = nodeHashes.get(mid);
            int cmp = midVal.compareTo(hash);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return low;
    }


    public void addRumour(String newRumour){
        rumours.put(newRumour,rumour_count);
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

            synchronized (neighbors){
                neighbors.add(socket);
            }

            return socket;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public String getNodeId() {
        return  nodeId;
    }

    public Map<String, Integer> getRumours(){
        return rumours;
    }

    public boolean containsNode(String nodeId) throws NoSuchAlgorithmException {
        String nodeIdHash = Hasher.md5(nodeId);
        return hashToNode.containsKey(nodeIdHash) || nodeId.equals(this.nodeId);
    }
}