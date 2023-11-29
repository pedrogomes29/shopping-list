package ShoppingList;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ShoppingListCRDT {

    private Map<String, CCounter> shoppingList;
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
        this.itemsList.setVersion(0);
        this.itemsList.setObservedIDs(new HashMap<>());
        for (CCounter cCounter: this.shoppingList.values()) {
            cCounter.setReplicaID(this.replicaID);
            cCounter.setVersion(0);
            cCounter.setObservedIDs(new HashMap<>());
            cCounter.setObservedCounters(new HashMap<>());
        }
    }

    public Map<String, CCounter> getShoppingList() {
        return this.shoppingList;
    }

    public AWORSet getItemsList() {
        return itemsList;
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

    public void merge(ShoppingListCRDT shoppingListCRDT) {
        Map<String, CCounter> mergedShoppingList = new HashMap<>();
        this.itemsList.merge(shoppingListCRDT.getItemsList());

        for (AWORSetElement item: this.itemsList.getItems()) {
            if (this.shoppingList.containsKey(item.getItem())) {
                if (shoppingListCRDT.getShoppingList().containsKey(item.getItem())) {
                    this.shoppingList.get(item.getItem())
                        .merge(shoppingListCRDT.getShoppingList().get(item.getItem()));
                }
                mergedShoppingList.put(item.getItem(), this.shoppingList.get(item.getItem()));
            } else {
                mergedShoppingList.put(item.getItem(), shoppingListCRDT.getShoppingList().get(item.getItem()));
            }
        }

        this.shoppingList = mergedShoppingList;
    }
}
