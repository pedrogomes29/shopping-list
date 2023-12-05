package Utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import java.util.Base64;

public class Serializer {

    public static String serializeBase64(Object o){
        byte[] objectBytes = serialize(o);

        if (objectBytes == null)
            return null;

        return Base64.getEncoder().encodeToString(objectBytes);
    }

    public static Object  deserializeBase64(String base64Object) {
        return deserialize(Base64.getDecoder().decode(base64Object));
    }

    public static byte[]  serialize(Object o){
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos)) {

            out.writeObject(o);
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            System.err.println("Error when serialize object: ");
            e.printStackTrace(System.err);
            return null;
        }
    }

    public static Object deserialize(byte[] serializedObjectBytes){
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedObjectBytes);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error when deserialize object: ");
            e.printStackTrace(System.err);
            return null;
        }
    }

}
