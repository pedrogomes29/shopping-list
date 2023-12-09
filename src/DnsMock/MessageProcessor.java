package DnsMock;

import NioChannels.Message.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Queue;

public class MessageProcessor extends NioChannels.Message.MessageProcessor{
    public MessageProcessor(Server server, Message message) {
        super(server, message);
    }

    private void dealWithNewLB(String messageContent) throws IOException {
        InetSocketAddress address = (InetSocketAddress) this.message.getSocket().socketChannel.getRemoteAddress();
        String newLBHost = address.getHostString();
        int newLBPort = Integer.parseInt(messageContent.split(" ")[1]);
        ((Server)server).addLoadBalancer(this.message.getSocket(), new InetSocketAddress(newLBHost,newLBPort));
    }

    @Override
    public void run() {
        String messageContent = new String(message.bytes);
        if (messageContent.startsWith("ADD_LB ")) {
            try {
                dealWithNewLB(messageContent);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else if (messageContent.equals("WHOIS LB")) {
            InetSocketAddress lbAddress = ((Server)server).getLoadBalancer();
            Queue<Message> writeQueue = server.getWriteQueue();
            synchronized (writeQueue) {
                writeQueue.add(new Message(lbAddress.getHostString()+":"+lbAddress.getPort(),message.getSocket()));
            }
        }
    }
}
