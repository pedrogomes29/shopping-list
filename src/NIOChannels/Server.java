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

    private final Map<Long, Socket> socketMap;


    public Server( int port, MessageProcessorBuilder messageProcessorBuilder )
    {
        this.port = port;
        this.messageProcessorBuilder = messageProcessorBuilder;
        messageProcessorBuilder.setServer(this);
        this.outboundMessageQueue = new LinkedList<>();
        this.socketQueue = new ArrayBlockingQueue<>(1024);
        this.socketMap = new HashMap<>();
    }

    public Queue<Message> getWriteQueue(){
        return this.outboundMessageQueue;
    }

    public Queue<Socket> getSocketQueue() { return this.socketQueue; }

    public Map<Long, Socket> getSocketMap() { return this.socketMap; }

    public void stopServer() throws InterruptedException
    {
        running = false;
        accepterThread.interrupt();
        accepterThread.join();
        processorThread.join();

    }

    public void propagateRequestWithClientId(Message requestMsg, Socket nodeSocket){
        long clientID = requestMsg.getSocket().getSocketId();

        byte[] request = requestMsg.bytes;
        String messageContent = new String(request);

        String[] requestParts = messageContent.split(" ");
        String requestMethod = requestParts[0];

        int requestBodyStartIdx = requestMethod.length() + 1;

        byte[] requestHeader = (requestMethod + " " + clientID + " ").getBytes();

        byte[] messageBytes = buildMessage(requestHeader, requestBodyStartIdx, request);

        synchronized (outboundMessageQueue) {
            outboundMessageQueue.offer(new Message(messageBytes,nodeSocket));
        }
    }

    public void propagateResponseWithoutClientId(byte[] request){
        String messageContent = new String(request);
        String[] requestParts = messageContent.split(" ");
        String requestMethod = requestParts[0];
        String clientIDStr = requestParts[1];
        long clientID = Long.parseLong(clientIDStr);

        int requestBodyStartIdx = requestMethod.length() + 1 + clientIDStr.length() + 1;

        byte[] requestHeader = (requestMethod + " ").getBytes();

        byte[] messageBytes = buildMessage(requestHeader, requestBodyStartIdx, request);

        Socket clientSocket = socketMap.get(clientID);

        synchronized (outboundMessageQueue) {
            outboundMessageQueue.offer(new Message(messageBytes,clientSocket));
        }
    }

    private byte[] buildMessage(byte[] requestHeader,int requestBodyStartIdx,byte[] request){
        byte[] lineSeparator = System.getProperty("line.separator").getBytes();

        int requestBodySize = request.length - requestBodyStartIdx;

        byte[] messageBytes = new byte[requestHeader.length + requestBodySize + lineSeparator.length];

        System.arraycopy(requestHeader,0,messageBytes,0,requestHeader.length);
        System.arraycopy(request,requestBodyStartIdx,messageBytes,requestHeader.length,requestBodySize);
        System.arraycopy(lineSeparator,0,messageBytes,requestHeader.length+requestBodySize,lineSeparator.length);

        return messageBytes;
    }


    public void startThreads()
    {
        try {
            SocketAccepter socketAccepter = new SocketAccepter(this, socketQueue);

            SocketProcessor socketProcessor = new SocketProcessor(this,socketQueue,this.messageProcessorBuilder,
                                                                    this.outboundMessageQueue, this.socketMap);
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