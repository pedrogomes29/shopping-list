package NIOChannels;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;



public abstract class Server
{
    public final int port;

    public boolean running = true;

    private final MessageProcessorBuilder messageProcessorBuilder;

    private Thread accepterThread;
    private  Thread processorThread;
    private final Queue<Message> outboundMessageQueue;
    private final Queue<Socket> socketQueue;
    public Server( int port, MessageProcessorBuilder messageProcessorBuilder )
    {
        this.port = port;
        this.messageProcessorBuilder = messageProcessorBuilder;
        messageProcessorBuilder.setServer(this);
        this.outboundMessageQueue = new LinkedList<>();
        this.socketQueue = new ArrayBlockingQueue<>(1024);
    }

    public Queue<Message> getWriteQueue(){
        return this.outboundMessageQueue;
    }

    public Queue<Socket> getSocketQueue() { return this.socketQueue; }

    public void stopServer() throws InterruptedException
    {
        running = false;
        accepterThread.interrupt();
        accepterThread.join();
        processorThread.join();

    }

    public void startThreads()
    {
        try {
            SocketAccepter socketAccepter = new SocketAccepter(this, socketQueue);

            SocketProcessor socketProcessor = new SocketProcessor(this,socketQueue, messageProcessorBuilder,this.outboundMessageQueue);
            accepterThread = new Thread(socketAccepter);
            processorThread = new Thread(socketProcessor);

            accepterThread.start();
            processorThread.start();
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }


}