package ShoppingList;

import java.util.HashMap;
import java.util.Map;

public class ShoppingListCRDT {
    Map<String, CCounter> shoppingList;
    AWORSet listItems;

    public ShoppingListCRDT() {
        this.shoppingList = new HashMap<>();
        this.listItems = new AWORSet();
    }

    public Map<String, CCounter> getShoppingList() {
        return this.shoppingList;
    }

    public void increment(String item, int quantity) {
        this.shoppingList.get(item).increment(quantity);
    }

    public void decrement(String item, int quantity) {
        if (this.shoppingList.get(item).getItemQuantity() < quantity) {
            this.listItems.remove(item);
            this.shoppingList.remove(item);
        } else {
            this.shoppingList.get(item).decrement(quantity);
        }
    }

    public void add(String item, int quantity) {
        this.listItems.add(item);
        this.shoppingList.put(item, new CCounter(quantity));
    }

    public void remove(String item) {
        this.listItems.remove(item);
        this.shoppingList.remove(item);
    }

    public void join(ShoppingListCRDT shoppingListCRDT) {
        // TODO
    }
}
