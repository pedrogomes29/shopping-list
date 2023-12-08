package NioChannels;

import NioChannels.Message.Message;
import NioChannels.Message.MessageProcessorBuilder;
import NioChannels.Socket.Socket;
import NioChannels.Socket.SocketAccepter;
import NioChannels.Socket.SocketProcessor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;


public class Server
{
    public final int port;
    public boolean running = true;

    protected final Map<Long, Socket> socketMap;
    private final SocketProcessor socketProcessor;
    private final SocketAccepter socketAccepter;

    // Threads
    private Thread accepterThread;
    private Thread processorThread;

    // Queues
    protected final Queue<Message> outboundMessageQueue;
    protected final Queue<Socket> socketQueue;
    protected final Queue<Socket> connectionQueue;



    public Server(int port, MessageProcessorBuilder messageProcessorBuilder ) throws IOException {
        this.port = port;
        messageProcessorBuilder.setServer(this);
        this.outboundMessageQueue = new LinkedList<>();
        this.socketQueue = new ArrayBlockingQueue<>(1024);
        this.socketMap = new ConcurrentHashMap<>();

        this.connectionQueue = new ConcurrentLinkedQueue<>();

        socketProcessor = new SocketProcessor(this,socketQueue, messageProcessorBuilder,
                this.outboundMessageQueue, this.socketMap, this.connectionQueue);
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
            return socket;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<Long, Socket> getSocketMap(){
        return socketMap;
    }

    public Queue<Message> getWriteQueue(){
        return outboundMessageQueue;
    }

}