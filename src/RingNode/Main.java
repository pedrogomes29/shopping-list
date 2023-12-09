package RingNode;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.UUID;

public class Main {

    public static void main(String[] args) throws IOException {
        boolean debug = false;

        if (args.length < 3)
            debug = true;

        String conf = "conf.txt";
        int port = 101;
        String id = "id2";


        if (!debug){
            id = args[0];
            port = Integer.parseInt(args[1]);
            conf = args[2];
        }


        Server server = new Server(id,"conf/" + conf, port, 3, 3);
        server.startThreads();

        Scanner scanner = new Scanner(System.in);
        String input;

        do {
            System.out.println("Server is running");
            System.out.println("Type 'close' to exit");
            input = scanner.nextLine();
        } while (!input.equals("close"));

        System.out.println("Server closed");

        try {
            server.stopServer();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }



}