package NIOChannels;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;


public abstract class Server
{
    public final int port;

    public boolean running = true;

    private Thread accepterThread;

    private  Thread processorThread;

    protected final Queue<Message> outboundMessageQueue;

    protected final Queue<Socket> socketQueue;

    protected final Map<Long, Socket> socketMap;

    private final SocketProcessor socketProcessor;
    private final SocketAccepter socketAccepter;
    private final ArrayList<Socket> neighborgs;

    public Queue<Message> getWriteQueue(){
        return outboundMessageQueue;
    }

    public Server( int port, MessageProcessorBuilder messageProcessorBuilder ) throws IOException {
        this.port = port;
        messageProcessorBuilder.setServer(this);
        this.outboundMessageQueue = new LinkedList<>();
        this.socketQueue = new ArrayBlockingQueue<>(1024);
        this.socketMap = new HashMap<>();
        this.neighborgs = new ArrayList<>();


        socketProcessor = new SocketProcessor(this,socketQueue, messageProcessorBuilder,
                this.outboundMessageQueue, this.socketMap);
        socketAccepter = new SocketAccepter(this, socketQueue);

    }

    public void stopServer() throws InterruptedException
    {
        running = false;
        accepterThread.interrupt();
        accepterThread.join();
        processorThread.join();

    }

    public void startThreads()
    {
        accepterThread = new Thread(socketAccepter);
        processorThread = new Thread(socketProcessor);

        accepterThread.start();
        processorThread.start();
    }

    public Queue<Socket> getSocketQueue(){
        return this.socketQueue;
    }

    public Socket connect(String host, int port){
        return connect(new InetSocketAddress(host, port));
    }

    public Socket connect(InetSocketAddress inetSocketAddress) {
        try {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            socketChannel.connect(inetSocketAddress);

            Socket socket = new Socket(socketChannel);

            synchronized (this.socketQueue) {
                socketQueue.offer(socket);
            }

            synchronized (this.neighborgs) {
                neighborgs.add(socket);
            }

            return socket;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}