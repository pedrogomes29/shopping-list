package DnsMock;

import java.io.IOException;

public class Main{

    public static void main(String[] args) throws IOException {
        int port = 9090;

        Server server = new Server( port );

        server.startThreads();

    }
}
