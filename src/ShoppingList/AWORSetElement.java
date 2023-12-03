package ShoppingList;

import java.io.Serializable;

public class AWORSetElement implements Serializable {

    private final String item;
    private int version;

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

    public void setVersion(int version) {
        this.version = version;
    }
}
