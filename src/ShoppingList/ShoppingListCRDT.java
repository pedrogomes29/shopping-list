package ShoppingList;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ShoppingListCRDT implements Serializable {
    Map<String, Integer> shoppingList;
    Map<String, Integer> currentShoppingList;
    Map<String, Integer> delta;

    public ShoppingListCRDT() {
        this.shoppingList = new HashMap<>();
        this.currentShoppingList = new HashMap<>();
        this.delta = new HashMap<>();
    }

    public ShoppingListCRDT(Map<String, Integer> shoppingList) {
        this.shoppingList = shoppingList;
        this.currentShoppingList = new HashMap<>(shoppingList);
        this.delta = new HashMap<>();
        shoppingList.forEach((item, quantity) -> {
            delta.put(item, 0);
        });
    }

    public Map<String, Integer> getShoppingList() {
        return this.shoppingList;
    }

    public Map<String, Integer> getCurrentShoppingList() {
        return this.currentShoppingList;
    }

    public Map<String, Integer> getDelta() {
        return this.delta;
    }

    public void increment(String item, int quantity) {
        this.currentShoppingList.put(item, this.currentShoppingList.get(item) + quantity);
        this.delta.put(item, this.delta.get(item) + quantity);
    }

    public void increment(String item) {
        increment(item, 1);
    }

    public void decrement(String item, int quantity) {
        if (this.currentShoppingList.get(item) < quantity) {
            if (this.shoppingList.containsKey(item)) {
                this.delta.put(item, -this.shoppingList.get(item));
            } else {
                this.delta.remove(item);
            }
            this.currentShoppingList.remove(item);
        } else {
            this.currentShoppingList.put(item, this.currentShoppingList.get(item) - quantity);
            this.delta.put(item, this.delta.get(item) - quantity);
        }
    }

    public void decrement(String item) {
        decrement(item, 1);
    }

    public boolean add(String item, int quantity) {
        if (this.currentShoppingList.containsKey(item)) {
            return false;
        }
        this.currentShoppingList.put(item, quantity);
        if (this.shoppingList.containsKey(item)) {
            this.delta.put(item, quantity - this.shoppingList.get(item));
        } else {
            this.delta.put(item, quantity);
        }
        return true;
    }

    public boolean reset(String item) {
        if (!this.currentShoppingList.containsKey(item)) {
            return false;
        }
        if (this.shoppingList.containsKey(item)) {
            this.delta.put(item, -this.shoppingList.get(item));
        } else {
            this.delta.remove(item);
        }
        this.currentShoppingList.remove(item);
        return true;
    }

    public void join(ShoppingListCRDT shoppingList2) {
        // TODO
    }
}
