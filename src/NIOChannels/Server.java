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


    public Server( int port, MessageProcessorBuilder messageProcessorBuilder )
    {
        this.port = port;
        this.messageProcessorBuilder = messageProcessorBuilder;
        messageProcessorBuilder.setServer(this);
    }


    public void stopServer() throws InterruptedException
    {
        running = false;
        System.out.println("accepter");
        accepterThread.interrupt();
        accepterThread.join();
        System.out.println("processor");
        processorThread.join();
        System.out.println("end");

    }

    public void startThreads()
    {
        try {
            Queue<Socket> socketQueue = new ArrayBlockingQueue<>(1024); //move 1024 to ServerConfig

            SocketAccepter socketAccepter = new SocketAccepter(this, socketQueue);

            SocketProcessor socketProcessor = new SocketProcessor(this,socketQueue, messageProcessorBuilder);
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