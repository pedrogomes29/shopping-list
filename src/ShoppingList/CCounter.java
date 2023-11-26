package ShoppingList;

public class CCounter {

    private int itemQuantity;

    CCounter() {
        this.itemQuantity = 0;
    }

    CCounter(int itemQuantity) {
        this.itemQuantity = itemQuantity;
    }

    public int getItemQuantity() {
        return itemQuantity;
    }

    public void increment(int quantity) {
        this.itemQuantity = this.itemQuantity + 1;
    }

    public void decrement(int quantity) {
        this.itemQuantity = this.itemQuantity - 1;
    }

    public void merge(CCounter cCounter) {
        this.itemQuantity = this.itemQuantity + cCounter.getItemQuantity();
    }
}
