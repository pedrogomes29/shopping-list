package Node;

import NIOChannels.Message;
import NIOChannels.Socket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.UUID;

public class Server extends NIOChannels.Server
{

    private Socket socketToLB;

    public Server( int nodePort )
    {
        super(nodePort, new MessageProcessorBuilder());
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

    public void sendMessageToLB(byte[] message){
        Queue<Message> writeQueue = this.getWriteQueue();
        synchronized (writeQueue) {
            writeQueue.add(new Message(message, socketToLB));
        }
    }
    public void sendMessageToLB(String message){
        Queue<Message> writeQueue = this.getWriteQueue();
        synchronized (writeQueue) {
            writeQueue.add(new Message(message, socketToLB));
        }
    }
}
