package ShoppingList;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class AWORSet implements Serializable {

    private List<AWORSetElement> items;
    private int version;
    private String replicaID;
    private Map<String, Integer> observedIDs;

    AWORSet(String replicaID) {
        this.version = 0;
        this.replicaID = replicaID;
        this.items = new ArrayList<>();
        this.observedIDs = new HashMap<>();
    }

    public List<AWORSetElement> getItems() {
        return this.items;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void incrementVersion() {
        this.version++;
        for (AWORSetElement item: this.items) {
            item.setVersion(this.version);
        }
    }

    public void setReplicaID(String replicaID) {
        this.replicaID = replicaID;
    }

    public void setObservedIDs(Map<String, Integer> observedIDs) {
        this.observedIDs = observedIDs;
    }

    public void add(String item) {
        this.version++;
        AWORSetElement newItem = new AWORSetElement(item, this.version);
        this.items.add(newItem);
    }

    public void remove(String item) {
        items.removeIf(element -> Objects.equals(element.getItem(), item));
    }

    public void merge(AWORSet aworSet) {
        List<AWORSetElement> mergedItems = new ArrayList<>();

        for (AWORSetElement item: this.items) {
            List<AWORSetElement> remoteItems = aworSet.items.stream()
                .filter(element -> Objects.equals(element.getItem(), item.getItem()))
                .collect(Collectors.toList());

            // item belongs in both replicas
            if (!remoteItems.isEmpty()) {
                AWORSetElement remoteItem = remoteItems.get(0);
                if (item.getVersion() >= remoteItem.getVersion()) {
                    mergedItems.add(item);
                } else {
                    mergedItems.add(remoteItem);
                }
            } else { // item belongs only in the local replica
                if (aworSet.observedIDs.get(this.replicaID) == null ||
                    item.getVersion() > aworSet.observedIDs.get(this.replicaID)) {
                    mergedItems.add(item);
                }
            }
        }

        for (AWORSetElement item: aworSet.items) {
            List<AWORSetElement> localItems = this.items.stream()
                .filter(element -> Objects.equals(element.getItem(), item.getItem()))
                .collect(Collectors.toList());

            // item belongs only in the remote replica
            if (localItems.isEmpty()) {
                if (this.observedIDs.get(aworSet.replicaID) == null ||
                    item.getVersion() > this.observedIDs.get(aworSet.replicaID)) {
                    mergedItems.add(item);
                }
            }
        }

        this.version++;
        this.observedIDs.put(aworSet.replicaID, aworSet.version);
        this.items = mergedItems;
    }
}
