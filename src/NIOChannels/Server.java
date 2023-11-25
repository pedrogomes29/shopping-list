package NIOChannels;


import LoadBalancer.TokenNode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;



public abstract class Server
{
    public final int port;

    public boolean running = true;

    private final MessageProcessorBuilder messageProcessorBuilder;

    private Thread accepterThread;
    private Thread processorThread;

    private Thread gossiperThread;

    static final int rumour_count = 3;

    protected final Map<String,Integer> rumours;
    protected final Queue<Message> outboundMessageQueue;
    protected final Queue<Socket> socketQueue;

    protected final Map<Long, Socket> socketMap;
    private final ArrayList<Socket> neighbors;

    private final ArrayList<String> nodeHashes;
    private final HashMap<String,TokenNode> hashToNode;

    protected String nodeId;



    public Queue<Message> getWriteQueue(){
        return outboundMessageQueue;
    }

    public Server( int port, MessageProcessorBuilder messageProcessorBuilder )
    {
        this.port = port;
        this.messageProcessorBuilder = messageProcessorBuilder;
        messageProcessorBuilder.setServer(this);
        this.outboundMessageQueue = new LinkedList<>();
        this.socketQueue = new ArrayBlockingQueue<>(1024);
        this.socketMap = new HashMap<>();
        this.neighbors = new ArrayList<>();
        this.nodeId = UUID.randomUUID().toString();
        this.rumours = new HashMap<>();
        this.nodeHashes = new ArrayList<>();
        this.hashToNode = new HashMap<>();
        addRumour(nodeId);
        connectToNeighborsFromConf("conf.txt");
    }

    public void addNodeToRing(TokenNode nodeId) throws NoSuchAlgorithmException {
        String nodeIdHash = nodeId.getHash();
        hashToNode.put(nodeIdHash,nodeId);
        int positionToInsert = binarySearch(nodeIdHash);
        nodeHashes.add(positionToInsert,nodeIdHash);
        for (String nodeHash : nodeHashes) System.out.println(nodeHash);
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

    public Socket connect(InetSocketAddress PLACEHOLDER){
        return null;
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
        try {
            SocketAccepter socketAccepter = new SocketAccepter(this, socketQueue);

            SocketProcessor socketProcessor = new SocketProcessor(this,socketQueue,this.messageProcessorBuilder,
                                                                    this.outboundMessageQueue, this.socketMap);

            Gossiper gossiper = new Gossiper(this,this.neighbors,this.rumours,this.outboundMessageQueue);

            accepterThread = new Thread(socketAccepter);
            processorThread = new Thread(socketProcessor);
            gossiperThread = new Thread(gossiper);
            accepterThread.start();
            processorThread.start();
            gossiperThread.start();
        }
        catch(IOException e){
            e.printStackTrace();
        }
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
            System.out.println("Failed to retrieve configuration file");
            e.printStackTrace();
        }
    }

}