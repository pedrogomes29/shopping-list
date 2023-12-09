package DnsMock;

import NioChannels.Socket.Socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class Server extends NioChannels.Server
{

    private final ArrayList<InetSocketAddress> loadBalancers;
    private final ConcurrentHashMap<Socket, InetSocketAddress> socketToAddr;
    private int roundRobinIdx;


    public Server(int port) throws IOException {
        super(port,new MessageProcessorBuilder());
        this.loadBalancers = new ArrayList<>();
        socketToAddr = new ConcurrentHashMap<>();
        this.roundRobinIdx = 0;
    }

    public InetSocketAddress getLoadBalancer(){
        InetSocketAddress lb = this.loadBalancers.get(roundRobinIdx);
        roundRobinIdx = (roundRobinIdx + 1) % loadBalancers.size();
        return lb;
    }

    public void addLoadBalancer(Socket socket,InetSocketAddress newLB){
        System.out.println("new load balancer: " + newLB.getAddress().toString() + ":" + newLB.getPort());
        this.loadBalancers.add(newLB);
        this.socketToAddr.put(socket, newLB);
    }


    @Override
    public void removeSocket(Socket socket) {
        super.removeSocket(socket);
        InetSocketAddress address = socketToAddr.get(socket);

        if ( address != null)
        {
            socketToAddr.remove(socket);
            loadBalancers.remove(address);
        }

    }
}
