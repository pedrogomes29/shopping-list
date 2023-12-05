package RingNode;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        boolean debug = false;
        int port = 101;
        String confFile = "conf1.txt";
        if (!debug){
            port =  Integer.parseInt(args[0]);
            confFile =  args[1];

        }

        Server server = new Server("conf/" + confFile, port, 3, 3);
        server.startThreads();
    }



}