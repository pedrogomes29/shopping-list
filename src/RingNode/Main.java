package RingNode;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        boolean debug = false;
        int port = 100;
        String confFile = "conf.txt";
        if (!debug){
            port =  Integer.parseInt(args[0]);
            confFile =  args[1];

        }

        Server server = new Server("conf/" + confFile, port, 3);
        server.startThreads();
    }
}