package ShoppingList;

import java.util.HashMap;
import java.util.Map;

public class ShoppingListCRDT {
    Map<String, Integer> shoppingList;
    Map<String, Integer> currentShoppingList;
    Map<String, Integer> delta;

    ShoppingListCRDT() {
        this.shoppingList = new HashMap<>();
        this.currentShoppingList = new HashMap<>();
        this.delta = new HashMap<>();
    }

    ShoppingListCRDT(Map<String, Integer> shoppingList) {
        this.shoppingList = shoppingList;
        this.currentShoppingList = shoppingList;
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

    // TODO: default quantity = 1
    public void increment(String item, int quantity) {
        // TODO
    }

    // TODO: default quantity = 1
    public void decrement(String item, int quantity) {
        // TODO
    }

    public void add(String item, int quantity) {
        // TODO
    }

    public void reset(String item) {
        // TODO
    }

    public void join(ShoppingListCRDT shoppingList2) {
        // TODO
    }
}
