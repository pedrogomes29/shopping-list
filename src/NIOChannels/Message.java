package NIOChannels;

public class Message {
    private long socketId;
    public byte[] bytes;
    public Message(byte[] bytes,long socketId) {
        this.bytes = bytes;
        this.socketId = socketId;
    }

    public Message(String message,long socketId) {
        this.bytes = (message+System.lineSeparator()).getBytes();
        this.socketId = socketId;
    }

    public long getSocketId(){
        return this.socketId;
    }
}
