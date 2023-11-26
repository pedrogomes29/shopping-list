package ShoppingList;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ShoppingListCRDT {

    private Map<String, CCounter> shoppingList;
    private AWORSet itemsList;
    private String replicaID;

    public ShoppingListCRDT() {
        this.shoppingList = new HashMap<>();
        this.itemsList = new AWORSet();
        this.replicaID = UUID.randomUUID().toString();
    }

    public void createNewID() {
        this.replicaID = UUID.randomUUID().toString();
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
        this.shoppingList.put(item, new CCounter(quantity));
    }

    public void remove(String item) {
        this.itemsList.remove(item);
        this.shoppingList.remove(item);
    }

    public void join(ShoppingListCRDT shoppingListCRDT) {
        // TODO
    }
}
