package Node;

import java.util.UUID;

/*
            UUID nodeId = UUID.randomUUID();
            out.println("ADD_NODE " + nodeId);
 */
public class Main {
    public static void main(String[] args) {
        Server server = new Server(8069);
        server.startThreads();
        server.connectToLB("localhost",8080);
        System.out.println("Connected to local host");
        String messageTxt = "ADD_NODE " + UUID.randomUUID();
        server.sendMessageToLB(messageTxt);
    }
}