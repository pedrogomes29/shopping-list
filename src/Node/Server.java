package Node;

import NIOChannels.Message;
import NIOChannels.Socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.sql.SQLException;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import Database.Database;

public class Server extends NIOChannels.Server
{

    private Socket socketToLB;
    private String nodeId;

    private Database db;

    public Server( int nodePort, String lbHost, int lbPort) throws IOException {
        super(nodePort, new MessageProcessorBuilder());
        connectToLB(lbHost, lbPort);
        nodeId = UUID.randomUUID().toString();
        String messageTxt = "ADD_NODE " + nodeId;
        try {
            db = new Database("database/"+nodeId+".db");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sendMessageToLB(messageTxt);
    }


    public void connectToLB(String LBHost, int LBPort ){
        SocketChannel socketChannelToLB;
        InetSocketAddress address = new InetSocketAddress(LBHost, LBPort);
        Queue<Socket> socketQueue = this.getSocketQueue();

        try{
            socketChannelToLB = SocketChannel.open();
            socketChannelToLB.connect(address);
            socketToLB = new Socket(socketChannelToLB);
            synchronized (socketQueue){
                socketQueue.add(socketToLB);
            }
        } catch(IOException e){
            e.printStackTrace();
        }
    }

    public Database getDB(){
        return db;
    }
        public void sendMessageToLB(String message){
        Queue<Message> writeQueue = this.getWriteQueue();
        synchronized (writeQueue) {
            writeQueue.add(new Message(message, socketToLB));
        }
    }


}
