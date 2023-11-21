package LoadBalencer;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Scanner;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) {
        Server server = new Server(8080);
        server.startThreads();

        Scanner scanner = new Scanner(System.in);
        String input;

        do {
            System.out.println("Server is running");
            System.out.println("Type 'close' to exit");
            input = scanner.nextLine();
            if(Objects.equals(input, "send")) {
                ArrayList<Integer> list = new ArrayList<>();
                list.add(1);
                list.add(2);
                list.add(3);
                try {
                    server.put("id1", list);
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
            }
        } while (!input.equals("close"));

        System.out.println("Server closed");

        try {
            server.stopServer();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}