package Client;

import ShoppingList.ShoppingListCRDT;
import Utils.Hasher;
import Utils.Serializer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;

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

    public static void main(String[] args) throws InterruptedException {

        // usado apenas para testar o client
        Client client = new Client();

        for(int i=0;i<10;i++) {
            ShoppingListCRDT list1CRDT = new ShoppingListCRDT();
            list1CRDT.add("bicicleta", 1);
            list1CRDT.add("pedro", 6);

            client.pushList(list1CRDT, "listadopedro-"+i);

        }

        Thread.sleep(5*1000);

        ShoppingListCRDT response1 = client.getList("listadopedro-0");

        System.out.println("listadopedro");
        for(String key : response1.getShoppingList().keySet()){
            System.out.println(key + " " + response1.getShoppingList().get(key).getItemQuantity());
        }

    }

    /*
    public static void main(String[] args) throws NoSuchAlgorithmException {

        // usado apenas para testar o client
        Client client = new Client();

        ShoppingListCRDT list1CRDT = new ShoppingListCRDT();
        list1CRDT.add("bicicleta", 1);
        list1CRDT.add("predro", 3);
        ShoppingListCRDT list1CRDTCopy = (ShoppingListCRDT) Serializer.deserializeBase64(Serializer.serializeBase64(list1CRDT));

        ShoppingListCRDT list2CRDT = new ShoppingListCRDT();
        list2CRDT.add("bicicleta", 1);
        list2CRDT.add("predro", 6);
        ShoppingListCRDT list2CRDTCopy = (ShoppingListCRDT) Serializer.deserializeBase64(Serializer.serializeBase64(list2CRDT));

        list1CRDT.merge(list2CRDTCopy);
        list2CRDT.merge(list1CRDTCopy);

        for(String key : list1CRDT.getShoppingList().keySet()){
            System.out.println(key + " " + list1CRDT.getShoppingList().get(key).getItemQuantity());
        }

        System.out.println(list1CRDT.equals(list2CRDT));
        System.out.println(Hasher.encodeAndMd5(list1CRDT));
        System.out.println(Hasher.encodeAndMd5(list2CRDT));

    }
    */
}