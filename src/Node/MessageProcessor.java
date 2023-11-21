package Node;

import NIOChannels.Message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;

public class MessageProcessor extends NIOChannels.MessageProcessor {
    public MessageProcessor(NIOChannels.Server server, Message message) {
        super(server, message);
    }

    @Override
    public void run() {
        String messageContent = new String(message.bytes);
        if(messageContent.startsWith("PUT")) {
            String[] messageContentStrings = messageContent.split(" ");
            String id = messageContentStrings[1];

            int nrSpaces = 0;
            int serializedObjectStartingIdx;
            for(serializedObjectStartingIdx=0;serializedObjectStartingIdx<message.bytes.length && nrSpaces<2;serializedObjectStartingIdx++){
                if(message.bytes[serializedObjectStartingIdx]==' ')
                    nrSpaces++;
            }
            byte[] serializedObject = Arrays.copyOfRange(message.bytes, serializedObjectStartingIdx, message.bytes.length);
            ((Node.Server)server).getDB().insertData(serializedObject);
            ByteArrayInputStream bis = new ByteArrayInputStream(serializedObject);
            ObjectInputStream in = null;
            try {
                in = new ObjectInputStream(bis);
                Object o = in.readObject();
                System.out.println("ID: " + " " + id);
                ArrayList<Integer> list = (ArrayList<Integer>) o;
                for (Integer integer : list) System.out.println(integer);
            }
            catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException ex) {
                    // ignore close exception
                }
            }
        }
    }
}
