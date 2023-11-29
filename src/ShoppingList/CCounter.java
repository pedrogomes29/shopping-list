package ShoppingList;

import java.util.HashMap;
import java.util.Map;

public class CCounter {

    private int itemQuantity;
    private int version;
    private String replicaID;
    private Map<String, Integer> observedIDs = new HashMap<>();
    private Map<String, Integer> observedCounters = new HashMap<>();

    CCounter(String replicaID) {
        this.itemQuantity = 0;
        this.version = 0;
        this.replicaID = replicaID;
        this.observedIDs.put(replicaID, this.version);
        this.observedCounters.put(replicaID, 0);
    }

    CCounter(int itemQuantity, String replicaID) {
        this(replicaID);
        this.itemQuantity = itemQuantity;
        this.version++;
        this.observedIDs.put(replicaID, this.version);
        this.observedCounters.put(replicaID, itemQuantity);
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void setReplicaID(String replicaID) {
        this.observedCounters.put(replicaID, this.observedCounters.get(this.replicaID));
        this.replicaID = replicaID;
        this.observedIDs.put(replicaID, this.version);
    }

    public void setObservedIDs(Map<String, Integer> observedIDs) {
        this.observedIDs = observedIDs;
    }

    public void setObservedCounters(Map<String, Integer> observedCounters) {
        this.observedCounters = observedCounters;
    }

    public int getItemQuantity() {
        return itemQuantity;
    }

    public Map<String, Integer> getObservedIDs() {
        return observedIDs;
    }

    public Map<String, Integer> getObservedCounters() {
        return observedCounters;
    }

    public void increment(int quantity) {
        this.itemQuantity = this.itemQuantity + quantity;
        this.version++;
        this.observedIDs.put(this.replicaID, this.version);
        this.observedCounters.put(this.replicaID, this.observedCounters.get(this.replicaID) + quantity);
    }

    public void decrement(int quantity) {
        this.itemQuantity = this.itemQuantity - quantity;
        this.version++;
        this.observedIDs.put(this.replicaID, this.version);
        this.observedCounters.put(this.replicaID, this.observedCounters.get(this.replicaID) - quantity);
    }

    public void merge(CCounter cCounter) {
        Map<String, Integer> mergedObservedIDs = new HashMap<>();
        Map<String, Integer> mergedObservedCounters = new HashMap<>();
        int mergedItemQuantity = 0;

        this.observedIDs.forEach((replicaID, version) -> {
            // replica belongs in both counters
            if (cCounter.getObservedIDs().containsKey(replicaID)) {
               if (cCounter.getObservedIDs().get(replicaID) > version){
                   mergedObservedIDs.put(replicaID, cCounter.getObservedIDs().get(replicaID));
                   mergedObservedCounters.put(replicaID, cCounter.getObservedCounters().get(replicaID));
               } else {
                   mergedObservedIDs.put(replicaID, version);
                   mergedObservedCounters.put(replicaID, this.observedCounters.get(replicaID));
               }
            } else {  // replica belongs only in the local counter
                mergedObservedIDs.put(replicaID, version);
                mergedObservedCounters.put(replicaID, this.observedCounters.get(replicaID));
            }
        });

        cCounter.getObservedIDs().forEach((replicaID, version) -> {
            // replica belongs only in the remote counter
            if (!this.observedIDs.containsKey(replicaID)) {
                mergedObservedIDs.put(replicaID, version);
                mergedObservedCounters.put(replicaID, cCounter.getObservedCounters().get(replicaID));
            }
        });

        for (Map.Entry<String, Integer> entry : mergedObservedCounters.entrySet()) {
            Integer quantity = entry.getValue();
            mergedItemQuantity += quantity;
        }

        this.observedIDs = mergedObservedIDs;
        this.observedCounters = mergedObservedCounters;
        this.itemQuantity = mergedItemQuantity;
    }
}
