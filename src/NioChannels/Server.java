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


public class Server implements AutoCloseable
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

    /**
     * Starts the threads responsible for accepting incoming socket connections and processing them.
     * This method creates and starts two threads: one for socket acceptance and another for socket processing.
     */
    public void startThreads()
    {
        accepterThread = new Thread(socketAccepter);
        processorThread = new Thread(socketProcessor);

        accepterThread.start();
        processorThread.start();
    }

    /**
     * Connects to a remote host using the specified host name and port number.
     *
     * @param host The host name or IP address of the remote server.
     * @param port The port number on the remote server.
     * @return A {@code Socket} object representing the established connection.
     */
    public Socket connect(String host, int port){
        return connect(new InetSocketAddress(host, port));
    }
    /**
     * Connects to a remote host using the provided {@code InetSocketAddress}.
     *
     * @param inetSocketAddress The {@code InetSocketAddress} containing the target host and port information.
     * @return A {@code Socket} object representing the established connection.
     */
    public Socket connect(InetSocketAddress inetSocketAddress) {
        try {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            socketChannel.connect(inetSocketAddress);

            Socket socket = new Socket(socketChannel);

            System.out.println("Connect :" + inetSocketAddress.toString());
            connectionQueue.offer(socket);
            return socket;

        } catch (IOException e) {
            System.err.println("Error: Cannot connect to " + inetSocketAddress);
            return null;
        }
    }

    public Map<Long, Socket> getSocketMap(){
        return socketMap;
    }

    public Queue<Message> getWriteQueue(){
        return outboundMessageQueue;
    }

    /**
     * Closes the resource.
     * @throws Exception if an error occurs during the closing process.
     */
    @Override
    public void close() throws Exception {
        stopServer();
    }

    public void removeSocket(Socket socket) {
        this.socketMap.remove(socket.getSocketId());

    }
}