package ShoppingList;

import java.util.*;
import java.util.stream.Collectors;

public class AWORSet {

    private List<AWORSetElement> items;
    private int version;
    private String replicaID;
    private final Map<String, Integer> observedIDs;

    AWORSet(String replicaID) {
        this.version = 0;
        this.replicaID = replicaID;
        this.items = new ArrayList<>();
        this.observedIDs = new HashMap<>();
    }

    public List<AWORSetElement> getItems() {
        return this.items;
    }

    public Map<String, Integer> getObservedIDs() {
        return this.observedIDs;
    }

    public void setReplicaID(String replicaID) {
        this.replicaID = replicaID;
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
            List<AWORSetElement> remoteItems = aworSet.getItems().stream()
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
                if (aworSet.getObservedIDs().get(this.replicaID) == null ||
                    item.getVersion() > aworSet.getObservedIDs().get(this.replicaID)) {
                    mergedItems.add(item);
                }
            }
        }

        for (AWORSetElement item: aworSet.getItems()) {
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

        this.observedIDs.put(aworSet.replicaID, aworSet.version);
        this.items = mergedItems;
    }
}
