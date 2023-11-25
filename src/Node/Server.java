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
    public final int nrReplicas;

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

    protected final ArrayList<String> nodeHashes;
    protected final HashMap<String,TokenNode> hashToNode;

    protected String nodeId;

    private final SocketProcessor socketProcessor;
    private final SocketAccepter socketAccepter;
    private final Gossiper gossiper;
    private final Set<Socket> neighbors;

    protected final Set<String> neighborIDs;


    public Queue<Message> getWriteQueue(){
        return outboundMessageQueue;
    }


    public Server(String confFilePath, int port,int nrReplicas, MessageProcessorBuilder messageProcessorBuilder ) throws IOException {
        this.port = port;
        this.nrReplicas = nrReplicas;
        messageProcessorBuilder.setServer(this);
        this.outboundMessageQueue = new LinkedList<>();
        this.socketQueue = new ArrayBlockingQueue<>(1024);
        this.socketMap = new ConcurrentHashMap<>();
        this.neighbors = new HashSet<>();
        this.neighborIDs = new HashSet<>();
        this.nodeId = UUID.randomUUID().toString();
        this.rumours = new HashMap<>();
        this.nodeHashes = new ArrayList<>();
        this.hashToNode = new HashMap<>();
        this.connectionQueue = new ConcurrentLinkedQueue<>();

        socketProcessor = new SocketProcessor(this,socketQueue, messageProcessorBuilder,
                this.outboundMessageQueue, this.socketMap, this.connectionQueue);
        socketAccepter = new SocketAccepter(this, socketQueue);

        gossiper = new Gossiper(this,this.neighbors,this.rumours,this.outboundMessageQueue);

        connectToNeighborsFromConf(confFilePath);
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

    protected int binarySearch(String hash) {
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

    public boolean knowsAboutRingNode(String nodeID) throws NoSuchAlgorithmException {
        String nodeIDHash = Hasher.md5(nodeID);
        return hashToNode.containsKey(nodeIDHash) || nodeID.equals(this.nodeId);
    }

    public void addLBNode(Socket socket,String socketID){
        synchronized (neighbors){
            neighbors.add(socket);
        }
        synchronized (neighborIDs){
            neighborIDs.add(socketID);
        }
    }

    public boolean knowsAboutLBNode(String socketID){
        return neighborIDs.contains(socketID);
    }

    /**
     * Propagates a request to the appropriate node using consistent hashing
     *
     * @param message The message containing the request and object ID.
     * @return The socket of the selected node to handle the request.
     * @throws NoSuchAlgorithmException If the required hashing algorithm is not available.
     */
    public Socket propagateRequestToNode(Message message) throws NoSuchAlgorithmException {
        int nrNodes = nodeHashes.size();
        String objectID = new String(message.bytes).split(" ")[1];
        String idHash = Utils.Hasher.md5(objectID);
        int firstNodeToStoreIdx = binarySearch(idHash);

        int randomChoice = ThreadLocalRandom.current().nextInt(firstNodeToStoreIdx, firstNodeToStoreIdx+nrReplicas-1);

        System.out.println("Random " + randomChoice + " total " + firstNodeToStoreIdx);
        String nodeHash = nodeHashes.get(randomChoice%nrNodes );
        TokenNode node = hashToNode.get(nodeHash);
        return node.getSocket();
    }


    public boolean isObjectReplica(String nodeId, String objectId) throws NoSuchAlgorithmException {
        int nrNodes = nodeHashes.size();
        String objectHash = Utils.Hasher.md5(objectId);
        String nodeHash = Utils.Hasher.md5(nodeId);
        int firstNodeToStoreIdx = binarySearch(objectHash);
        System.out.println("objectHash: " + objectHash);
        System.out.println("nodeHash: " + nodeHash);

        for (int i = 0; i < nrReplicas; i++){
            int idx = (firstNodeToStoreIdx+i)%nrNodes;

            System.out.println( idx + " - " + nodeHashes.get(idx));
            if(nodeHashes.get(idx).equals(nodeHash))
                return true;
        }

        return false;
    }

    public Map<Long, Socket> getSocketMap(){
        return socketMap;
    }
}