package RingNode;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws SQLException, IOException {
        boolean debug = true;
        int port = 101;
        String confFile = "conf1.txt";
        if (!debug){
            port =  Integer.parseInt(args[0]);
            confFile =  args[1];

        }

        Server server = new Server("conf/" + confFile, port, 3, 3);
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