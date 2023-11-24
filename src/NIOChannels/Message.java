package NIOChannels;

public class Message {
    private final Socket socket;
    public byte[] bytes;
    public Message(byte[] bytes,Socket socket) {
        this.bytes = bytes;
        this.socket = socket;
    }
    public Message(String message,Socket socket) {
        this.bytes = (message+'\n').getBytes();
        this.socket = socket;
    }

    public Socket getSocket(){
        return this.socket;
    }
}
