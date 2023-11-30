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

    public int getItemQuantity() {
        return itemQuantity;
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
        int mergedItemQuantity = 0;

        this.observedIDs.forEach((replicaID, version) -> {
            // replica belongs in both counters
            if (cCounter.observedIDs.containsKey(replicaID)) {
               if (cCounter.observedIDs.get(replicaID) > version) {
                   this.observedIDs.put(replicaID, cCounter.observedIDs.get(replicaID));
                   this.observedCounters.put(replicaID, cCounter.observedCounters.get(replicaID));
               }
            }
        });

        cCounter.observedIDs.forEach((replicaID, version) -> {
            // replica belongs only in the remote counter
            if (!this.observedIDs.containsKey(replicaID)) {
                this.observedIDs.put(replicaID, version);
                this.observedCounters.put(replicaID, cCounter.observedCounters.get(replicaID));
            }
        });

        for (Map.Entry<String, Integer> counter: observedCounters.entrySet()) {
            mergedItemQuantity += counter.getValue();
        }
        this.version++;
        this.itemQuantity = mergedItemQuantity;
    }
}
