package NioChannels.Socket;
import NioChannels.Message.Message;
import NioChannels.Message.MessageProcessor;
import NioChannels.Message.MessageProcessorBuilder;
import NioChannels.Message.MessageWriter;
import NioChannels.Server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.util.HashSet;
import java.util.Set;
import java.util.Queue;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Iterator;
import java.util.List;

public class SocketProcessor implements Runnable{
    private final Queue<Socket> inboundSocketQueue;
    private long nextSocketId = 0;
    private final Selector readSelector;
    private final Selector writeSelector;

    private final Selector connectorSelector;
    private final Server server;

    private final ByteBuffer readByteBuffer  = ByteBuffer.allocate(1024 * 1024);
    private final ByteBuffer writeByteBuffer = ByteBuffer.allocate(1024 * 1024);
    private final Set<Socket> emptyToNonEmptySockets = new HashSet<>();
    private final Set<Socket> nonEmptyToEmptySockets = new HashSet<>();
    private final Queue<Message> outboundMessageQueue;

    private final Map<Long, Socket> socketMap;
    private final ExecutorService processorThreadPool;

    private final MessageProcessorBuilder messageProcessorBuilder;

    private final Queue<Socket> connectionQueue;

    public SocketProcessor(Server server, Queue<Socket> inboundSocketQueue,
                           MessageProcessorBuilder messageProcessorBuilder, Queue<Message> outboundMessageQueue, Map<Long, Socket> socketMap, Queue<Socket> connectionQueue) throws IOException{
        this.inboundSocketQueue = inboundSocketQueue;
        this.outboundMessageQueue = outboundMessageQueue;
        this.readSelector = Selector.open();
        this.writeSelector = Selector.open();
        this.connectorSelector = Selector.open();
        this.server = server;
        this.processorThreadPool = Executors.newFixedThreadPool(10);
        this.messageProcessorBuilder = messageProcessorBuilder;
        this.socketMap = socketMap;
        this.connectionQueue = connectionQueue;
    }


    @Override
    public void run() {
        int retry = 0;
        while(server.running && retry < 3){
            try{
                executeCycle();
                retry = 0;
            } catch(IOException e){
                if (retry == 0)
                    System.err.println("Error: socket processor. " + e.getMessage());
                retry+=1;
                System.out.println("Retry " + retry + "/3");
            }
        }

        if (retry == 3){
            throw new RuntimeException("Socket processor failed 3 times");
        }
    }


    private void executeCycle() throws IOException {
        registerNewConnections();
        takeNewConnections();
        takeNewSockets();
        try {
            readFromSockets();
        }catch (Exception ex){
            ex.printStackTrace();
        }
        writeToSockets();
    }


    public void registerNewConnections() {
        Socket newSocket;
        newSocket = this.connectionQueue.poll();

        while(newSocket != null){
            SelectionKey key= null;
            try {
                newSocket.socketChannel.configureBlocking(false);

                key = newSocket.socketChannel.register(this.connectorSelector, SelectionKey.OP_CONNECT);
                key.attach(newSocket);
            }catch (Exception exception){
                System.err.println("Error: Register connecting");
                closeSocket(key);

            }

            newSocket = this.connectionQueue.poll();
        }
    }

    /**
     * Initiates the acceptance of new incoming connections using a non-blocking selector.
     * This method completes the connection of sockets that are ready to connect,
     * and adds the sockets to an inbound socket queue for further processing.
     *
     * @throws IOException if an I/O error occurs while handling the incoming connections.
     */
    public void takeNewConnections() throws IOException {
        int connectionReady = this.connectorSelector.selectNow();

        if(connectionReady > 0){
            Set<SelectionKey> selectedKeys = this.connectorSelector.selectedKeys();
            Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

            while(keyIterator.hasNext()) {
                SelectionKey key = keyIterator.next();
                Socket socket = (Socket) key.attachment();
                try {

                    socket.socketChannel.finishConnect();

                    synchronized (this.inboundSocketQueue) {
                        this.inboundSocketQueue.offer(socket);
                    }
                }catch (Exception exception){
                    System.err.println("Error: Error connecting");
                    closeSocket(key);
                }

                keyIterator.remove();
            }
            selectedKeys.clear();
        }
    }

    /**
     * Processes new sockets from the inbound socket queue, configures them for non-blocking
     * communication, registers them for read operations, and adds them to a socket map for
     * tracking and management.
     */
    public void takeNewSockets(){

        Socket newSocket;
        synchronized (this.inboundSocketQueue) {
            newSocket = this.inboundSocketQueue.poll();
        }
        while(newSocket != null){
            try {
                long newSocketId = nextSocketId+1;
                newSocket.socketChannel.configureBlocking(false);
                newSocket.setSocketId(newSocketId);

                SelectionKey key = newSocket.socketChannel.register(this.readSelector, SelectionKey.OP_READ);
                this.socketMap.put(newSocket.getSocketId(), newSocket);
                key.attach(newSocket);

                synchronized (this.inboundSocketQueue) {
                    newSocket = this.inboundSocketQueue.poll();
                }

                ++nextSocketId;
            }catch (Exception e){
                System.err.println("Error: Error accepting new sockets connections");
            }
        }
    }

    public void readFromSockets() throws IOException {
        int readReady = this.readSelector.selectNow();

        if(readReady > 0){
            Set<SelectionKey> selectedKeys = this.readSelector.selectedKeys();
            Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

            while(keyIterator.hasNext()) {
                SelectionKey key = keyIterator.next();

                try {
                    readFromSocket(key);
                }catch (IOException e){
                    System.err.println("Error: read from socket");
                    closeSocket(key);

                }
                keyIterator.remove();
            }
            selectedKeys.clear();
        }
    }

    protected void closeSocket(SelectionKey key){
        if (key == null)
            return;

        Socket socket = (Socket) key.attachment();
        if (socket == null)
            return;

        System.out.println("Socket closed: " + socket.getSocketId());
        server.removeSocket(socket);
        try {
            socket.close();
        } catch (IOException e) {
            System.err.println("Error: IOException while closing socket");
        }finally {
            key.cancel();
        }
    }

    private void readFromSocket(SelectionKey key) throws IOException {
        Socket socket = (Socket) key.attachment();

        int readed = socket.read(this.readByteBuffer);
        System.out.println(readed);
        socket.messageReader.read(this.readByteBuffer);
        List<Message> fullMessages = socket.messageReader.messages;
        if (!fullMessages.isEmpty()) {
            for (Message message : fullMessages) {
                System.out.println("Read: " + new String(message.bytes));
                MessageProcessor messageProcessor = this.messageProcessorBuilder.build(message);
                processorThreadPool.execute(messageProcessor);  //the message processor will eventually push outgoing messages into a MessageWriter for this socket.
            }

            fullMessages.clear();
        }

        if (socket.endOfStreamReached)
            closeSocket(key);

    }

    public void writeToSockets() throws IOException {

        // Take all new messages from outboundMessageQueue
        takeNewOutboundMessages();

        // Cancel all sockets which have no more data to write.
        cancelEmptySockets();

        // Register all sockets that *have* data and which are not yet registered.
        registerNonEmptySockets();

        // Select from the Selector.
        int writeReady = this.writeSelector.selectNow();

        if(writeReady > 0){
            Set<SelectionKey>      selectionKeys = this.writeSelector.selectedKeys();
            Iterator<SelectionKey> keyIterator   = selectionKeys.iterator();

            while(keyIterator.hasNext()){
                SelectionKey key = keyIterator.next();

                Socket socket = (Socket) key.attachment();
                System.out.println("writeByteBuffer");
                System.out.println(this.writeByteBuffer);
                socket.messageWriter.write(this.writeByteBuffer);

                if(socket.messageWriter.isEmpty()){
                    this.nonEmptyToEmptySockets.add(socket);
                }

                keyIterator.remove();
            }

            selectionKeys.clear();

        }
    }

    private void registerNonEmptySockets() throws ClosedChannelException {
        for(Socket socket : emptyToNonEmptySockets) {
            if (socket.socketChannel.isConnected())
                socket.socketChannel.register(this.writeSelector, SelectionKey.OP_WRITE, socket);
        }
        emptyToNonEmptySockets.clear();
    }

    private void cancelEmptySockets() {
        for(Socket socket : nonEmptyToEmptySockets){
            SelectionKey key = socket.socketChannel.keyFor(this.writeSelector);

            key.cancel();
        }
        nonEmptyToEmptySockets.clear();
    }

    private void takeNewOutboundMessages() {
        Message outMessage;
        synchronized (outboundMessageQueue) {
            outMessage = this.outboundMessageQueue.poll();
        }
        while(outMessage != null){
            Socket socket = outMessage.getSocket();

            if(socket != null && !socket.isClosed()){
                MessageWriter messageWriter = socket.messageWriter;
                if(messageWriter.isEmpty()){
                    messageWriter.enqueue(outMessage);
                    nonEmptyToEmptySockets.remove(socket);
                    emptyToNonEmptySockets.add(socket);
                } else{
                    messageWriter.enqueue(outMessage);
                }
            }
            synchronized (outboundMessageQueue) {
                outMessage = this.outboundMessageQueue.poll();
            }
        }
    }

}
