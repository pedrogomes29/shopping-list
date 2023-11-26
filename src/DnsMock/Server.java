package DnsMock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

public class Server extends NioChannels.Server
{

    private final ArrayList<InetSocketAddress> loadBalancers;
    private int roundRobinIdx;


    public Server(int port) throws IOException {
        super(port,new MessageProcessorBuilder());
        this.loadBalancers = new ArrayList<>();
        this.roundRobinIdx = 0;
    }

    public InetSocketAddress getLoadBalancer(){
        InetSocketAddress lb = this.loadBalancers.get(roundRobinIdx);
        roundRobinIdx = (roundRobinIdx + 1) % loadBalancers.size();
        return lb;
    }

    public void addLoadBalancer(InetSocketAddress newLB){
        System.out.println("new load balancer: " + newLB.getAddress().toString() + ":" + newLB.getPort());
        this.loadBalancers.add(newLB);
    }
}
