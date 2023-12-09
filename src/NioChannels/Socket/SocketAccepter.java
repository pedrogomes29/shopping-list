package NioChannels.Socket;

import NioChannels.Server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Queue;

public class SocketAccepter implements Runnable{
    private final Server server;
    private final Queue<Socket> socketQueue;

    public SocketAccepter(Server server, Queue<Socket> socketQueue)  {
        this.server     = server;
        this.socketQueue = socketQueue;
    }



    public void run() {
        ServerSocketChannel serverSocket;
        try{
            serverSocket = ServerSocketChannel.open();
            serverSocket.bind(new InetSocketAddress(server.port));
        } catch(Exception e){
            System.err.println("Erro: Cannot bind port " + server.port + ". " + e.getMessage());
            return;
        }


        while(server.running){
            try{
                SocketChannel socketChannel = serverSocket.accept();

                System.out.println("Socket accepted: " + socketChannel);
                synchronized (this.socketQueue) {
                    this.socketQueue.add(new Socket(socketChannel));
                }

            }catch(Exception e){
                System.err.println("Error: Exception while accept new socket " + e.getMessage());
            }

        }

    }
}