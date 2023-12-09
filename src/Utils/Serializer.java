package Utils;

import ShoppingList.CCounter;
import ShoppingList.ShoppingListCRDT;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.util.Map;

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
            e.printStackTrace();
            return null;
        }
    }

    public static String encodeShoppingListCRDTPublicAttributes(ShoppingListCRDT shoppingListCRDT){
        StringBuilder serializedShoppingList = new StringBuilder();
        Map<String, CCounter> shoppingList = shoppingListCRDT.getShoppingList();
        for(String shoppingListItemName:shoppingList.keySet().stream().sorted().toList()){
            CCounter shoppingListItem = shoppingList.get(shoppingListItemName);
            serializedShoppingList.append(shoppingListItemName).append(encodeCCounterPublicAttributes(shoppingListItem));
        }
        return serializedShoppingList.toString();
    }

    public static String encodeCCounterPublicAttributes(CCounter counter){
        return counter.getItemQuantity() + serializeBase64(counter.getObservedIDs()) +
                serializeBase64(counter.getObservedCounters());
    }

    public static Object deserialize(byte[] serializedObjectBytes){
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedObjectBytes);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

}
