package Client;

import ShoppingList.ShoppingListCRDT;
import Utils.Serializer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Client {
    private Socket socketToLB;
    private OutputStream socketOutput;
    private InputStream socketInput;

    public Client(){

    }


    public InetSocketAddress getCloudIP(String dnsHost, int dnsPort){
        InetSocketAddress address = new InetSocketAddress(dnsHost, dnsPort);

        try{
            socketToLB = new Socket();
            socketToLB.connect(address);
            socketToLB.setSoTimeout(100000);

            socketOutput = socketToLB.getOutputStream();
            socketInput = socketToLB.getInputStream();

            String message = "WHOIS LB" + "\n";
            socketOutput.write(message.getBytes());

            String serverResponse = readLine(100000);
            String[] serverResponseParts = serverResponse.split(":");
            String lbHost = serverResponseParts[0];
            int lbPort = Integer.parseInt(serverResponseParts[1]);

            return new InetSocketAddress(lbHost,lbPort);
        } catch(IOException e){
            e.printStackTrace();
            return null;
        }
    }
    public void connectToCloud(String dnsHost, int dnsPort){
        InetSocketAddress address = getCloudIP(dnsHost,dnsPort);
        if(address==null)
            return;
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
        connectToCloud("127.0.0.1", 9090);

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

    public ShoppingListCRDT getList(String id) {
        connectToCloud("127.0.0.1", 9090);

        byte[] getmessageBytes = ("GET " + id + "\n").getBytes();

        try {
            // ask for crdt to cloud
            socketOutput.write(getmessageBytes);

            // recv crdt
            while (true) {
                String serverResponse = readLine(10000);

                String[] response = serverResponse.split(" ");

                if (response[0].equals("GET_RESPONSE") && response[1].equals(id)) {

                    if (response[2].equals("null"))
                        return null;

                    Object returnedObject = Serializer.deserializeBase64(response[2]);

                    if (returnedObject instanceof ShoppingListCRDT)
                        return (ShoppingListCRDT) returnedObject;
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

        // usado apenas para testar o client
        Client client = new Client();

        ShoppingListCRDT listCRDT = new ShoppingListCRDT();
        listCRDT.add("bicicleta", 1);
        listCRDT.add("predro", 2);

        boolean sent = client.pushList(listCRDT, "listadopedro");

        System.out.println("Sent" + sent);

        String listaName = "listadorui";
        ShoppingListCRDT response = client.getList(listaName);

        if (response == null){
            System.out.println("List " + listaName + " doesnt exist");
            return;
        }

        for(String key : response.getShoppingList().keySet()){
            System.out.println(key + " " + response.getShoppingList().get(key).getItemQuantity());
        }
    }

}