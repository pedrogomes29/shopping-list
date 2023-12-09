package RingNode;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.UUID;

public class Main {

    public static void main(String[] args) throws SQLException, IOException {

        boolean debug = true;

        String conf = "conf1.txt";
        int port = 103;
        String id = UUID.randomUUID().toString();


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