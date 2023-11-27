package ShoppingList;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ShoppingListCRDT {

    private final Map<String, CCounter> shoppingList;
    private final AWORSet itemsList;
    private String replicaID;

    public ShoppingListCRDT() {
        this.replicaID = UUID.randomUUID().toString();
        this.shoppingList = new HashMap<>();
        this.itemsList = new AWORSet(replicaID);
    }

    public void createNewID() {
        this.replicaID = UUID.randomUUID().toString();
        this.itemsList.setReplicaID(this.replicaID);
        for(CCounter cCounter : this.shoppingList.values()){
            cCounter.setReplicaID(replicaID);
        }
    }

    public Map<String, CCounter> getShoppingList() {
        return this.shoppingList;
    }

    public void increment(String item, int quantity) {
        this.shoppingList.get(item).increment(quantity);
    }

    public void decrement(String item, int quantity) {
        if (this.shoppingList.get(item).getItemQuantity() < quantity) {
            this.itemsList.remove(item);
            this.shoppingList.remove(item);
        } else {
            this.shoppingList.get(item).decrement(quantity);
        }
    }

    public void add(String item, int quantity) {
        this.itemsList.add(item);
        this.shoppingList.put(item, new CCounter(quantity, replicaID));
    }

    public void remove(String item) {
        this.itemsList.remove(item);
        this.shoppingList.remove(item);
    }

    public void join(ShoppingListCRDT shoppingListCRDT) {
        // TODO
    }
}
