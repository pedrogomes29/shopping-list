package RingNode;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        int port =  Integer.parseInt(args[0]);
        //int port =  100;

        String confFile =  args[1];
        //String confFile =  "conf1.txt";

        Server server = new Server("conf/" + confFile, port);
        server.startThreads();
    }
}