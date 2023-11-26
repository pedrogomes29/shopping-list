package ShoppingList;

public class AWORSetElement {

    private final String item;
    private final int version;

    AWORSetElement(String item, int version) {
        this.item = item;
        this.version = version;
    }

    public String getItem() {
        return item;
    }

    public int getVersion() {
        return version;
    }
}
