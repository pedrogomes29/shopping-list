package Admin;

import Node.ConsistentHashing.TokenNode;

import java.io.IOException;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws IOException {
        boolean debug = false;
        int port = 7070;
        String confFile = "conf.txt";
        if (!debug) {
            port =  Integer.parseInt(args[0]);
            confFile =  args[1];
        }

        Server server = new Server("conf/" + confFile, port, 0, 3);
        server.startThreads();

        Scanner scanner = new Scanner(System.in);
        String input;
        boolean foundNode = false;
        do {
            System.out.println("Server is running");
            System.out.println("Specify node to remove");
            System.out.println("Type 'close' to exit");
            input = scanner.nextLine();

            if (!input.equals("close")) {
                for (TokenNode tokenNode: server.consistentHashing.getHashToNode().values()) {
                    if (input.equals(tokenNode.getId())) {
                        server.gossiper.addRumour("REMOVE " + input);
                        foundNode = true;
                        break;
                    }
                }
                if (!foundNode) {
                    System.out.println("Couldn't find node " + input);
                }
                foundNode = false;
            }
        } while(!input.equals("close"));
        System.out.println("Server closed");

        try {
            server.stopServer();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
