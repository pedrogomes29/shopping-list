package Client;


import ShoppingList.ShoppingListCRDT;
import Utils.Serializer;
import jdk.jshell.spi.ExecutionControl;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Queue;

public class Client {
    private Socket socketToLB;
    private OutputStream socketOutput;
    private InputStream socketInput;

    public Client(){

    }

    public void connectToCloud(String lbHost, int lbPort){
        InetSocketAddress address = new InetSocketAddress(lbHost, lbPort);

        try{
            socketToLB = new Socket();
            socketToLB.connect(address);
            socketToLB.setSoTimeout(100000);

            socketOutput = socketToLB.getOutputStream();
            socketInput = socketToLB.getInputStream();
        } catch(IOException e){
            e.printStackTrace();
        }
    }

    public boolean pushList(Object crdt, String id) {
        String message = "PUT " + id + " " + Serializer.serializeBase64(crdt) + "\n";

        try {
            // send crdt to cloud
            socketOutput.write(message.getBytes());

            // recv socket confirmation
            String serverResponse = readLine(100000);

            // Check if the server response is THE ACK
            return serverResponse.equals("PUT_ACK " + id);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }
    public String readLine(long timeout) throws IOException {
        StringBuilder response = new StringBuilder();

        long limitTime = System.currentTimeMillis() + timeout; // 10 seconds timeout
        int c;

        while (System.currentTimeMillis() < limitTime && (c = socketInput.read()) >= 0) {
            char character = (char) c;
            if (character == '\n') {
                return response.toString();
            }
            response.append(character);
        }

        return null;
    }

    public Object getList(String id) {
        byte[] getmessageBytes = ("GET " + id + "\n").getBytes();

        try {
            // ask for crdt to cloud
            socketOutput.write(getmessageBytes);

            // recv crdt
            while (true) {
                String serverResponse = readLine(10000);

                String[] response = serverResponse.split(" ");

                if (!response[0].equals("GET_RESPONSE") || !response[1].equals(id))
                    continue;
                else {
                    Object returnedObject = Serializer.deserializeBase64(response[2]);

                    if (returnedObject instanceof Object)
                        return returnedObject;
                    break;
                }
            }

            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }

    public static void main(String[] args) {
        Client client = new Client();

        client.connectToCloud("127.0.0.1", 8080);

        ShoppingListCRDT listCRDT = new ShoppingListCRDT();
        listCRDT.add("bicicleta", 1);

        boolean sended = client.pushList(listCRDT, "listadorui");

        ShoppingListCRDT response = (ShoppingListCRDT)client.getList("lista");

        for(String key : response.getCurrentShoppingList().keySet()){
            System.out.println(key + " " + response.getCurrentShoppingList().get(key));
        }
    }

}