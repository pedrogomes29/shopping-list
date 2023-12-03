package RingNode;

import java.io.IOException;
import java.sql.SQLException;

public class Main {

    public static void main(String[] args){
        boolean debug = false;
        int port = 101;
        String confFile = "conf1.txt";
        if (!debug){
            port =  Integer.parseInt(args[0]);
            confFile =  args[1];

        }

        try(Server server = new Server("conf/" + confFile, port, 3, 3)) {
            server.startThreads();

        }catch (Exception exception){
            System.err.println(exception.getMessage());
        }
    }
}