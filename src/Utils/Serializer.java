package Utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;

public class Serializer {

    byte[] serialize(Object o) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(o);
        out.flush();
        byte[] serializedObject =  bos.toByteArray();
        bos.close();
        return serializedObject;
    }

    Object deserialize(byte[] serializedObjectBytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(serializedObjectBytes);
        ObjectInputStream in = new ObjectInputStream(bis);
        Object o = in.readObject();
        in.close();
        return o;
    }

}
