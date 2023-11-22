package Node;

import NIOChannels.Message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import Utils.Hasher;

public class MessageProcessor extends NIOChannels.MessageProcessor {
    public MessageProcessor(NIOChannels.Server server, Message message) {
        super(server, message);
    }

    @Override
    public void run() {
        String messageContent = new String(message.bytes);
        if(messageContent.startsWith("PUT")) {
            String[] messageContentStrings = messageContent.split(" ");
            String clientID = messageContentStrings[1];
            String objectID = messageContentStrings[2];
            int nrSpaces = 0;
            int serializedObjectStartingIdx;
            for(serializedObjectStartingIdx=0;serializedObjectStartingIdx<message.bytes.length && nrSpaces<3;serializedObjectStartingIdx++){
                if(message.bytes[serializedObjectStartingIdx]==' ')
                    nrSpaces++;
            }
            String hash = null;
            try {
                hash = Hasher.md5(objectID);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            byte[] serializedObject = Arrays.copyOfRange(message.bytes, serializedObjectStartingIdx, message.bytes.length);
            ((Node.Server)server).getDB().insertData(hash,serializedObject);
            /*
            TODO: MANDAR ACK STORED OBJECT
             */
            ByteArrayInputStream bis = new ByteArrayInputStream(serializedObject);
            ObjectInputStream in = null;
            try {
                in = new ObjectInputStream(bis);
                Object o = in.readObject();
                System.out.println("ID: " + " " + objectID);
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
