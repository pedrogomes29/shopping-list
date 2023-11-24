package Node;

import java.util.UUID;

public class Main {
    public static void main(String[] args) {
        Server server = new Server(100 ,"localhost", 8080);
        server.startThreads();
    }
}