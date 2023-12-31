package Admin;

import Node.ConsistentHashing.TokenNode;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Scanner;
import java.util.UUID;

public class Main {

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {

        boolean debug = false;
        if (args.length < 3)
            debug = true;

        int port = 7070;
        String confFile = "conf.txt";
        String id = UUID.randomUUID().toString();

        if (!debug) {
            id = args[0];
            port =  Integer.parseInt(args[1]);
            confFile =  args[2];
        }

        Server server = new Server(id,"conf/" + confFile, port, 0, 3);
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
                        server.consistentHashing.removeNodeFromRing(tokenNode);
                        foundNode = true;
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
