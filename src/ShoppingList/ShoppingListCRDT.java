package ShoppingList;

import java.util.HashMap;
import java.util.Map;

public class ShoppingListCRDT {
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
        Map<String, Integer> delta2 = shoppingList2.getDelta();

        // Adapt delta of shoppingList2 according to this.shoppingList if necessary
        if (!shoppingList2.getShoppingList().equals(this.shoppingList)) {
            shoppingList2.getDelta().forEach((item, quantity) -> {
                Integer newQuantity = quantity + shoppingList2.getShoppingList().getOrDefault(item, 0) - this.shoppingList.getOrDefault(item, 0);
                delta2.put(item, newQuantity);
            });
        }
        for (Map.Entry<String, Integer> entry : delta2.entrySet()) {
            String item = entry.getKey();
            Integer quantity = entry.getValue();

            if (!this.delta.containsKey(item)) {
                this.add(item, quantity);
            } else {
                int newDelta, deltaOffset;

                if ((this.delta.get(item) >= 0 && delta2.get(item) >= 0) || (this.delta.get(item) <= 0 && delta2.get(item) <= 0)) {
                    newDelta = Math.max(delta2.get(item), this.delta.get(item));
                } else {
                    newDelta = delta2.get(item) + this.delta.get(item);
                }
                deltaOffset = newDelta - this.delta.get(item);
                if (deltaOffset > 0) {
                    this.increment(item, deltaOffset);
                } else {
                    this.decrement(item, Math.abs(deltaOffset));
                }
            }
        }
    }
}
