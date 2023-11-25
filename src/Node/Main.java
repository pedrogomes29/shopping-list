package Node;

import java.io.IOException;
public class Main {
    public static void main(String[] args) throws IOException {
        Server server = new Server(100 ,"localhost", 8080);
        server.startThreads();
    }
}