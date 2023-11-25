package Node.Gossiper;

public enum ALREADY_SEEN_RUMOUR {
    FALSE("0"),
    TRUE("1");

    private final String value;
    ALREADY_SEEN_RUMOUR(String value){
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}