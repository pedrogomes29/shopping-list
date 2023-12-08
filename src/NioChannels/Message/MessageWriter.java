package NioChannels.Message;

import NioChannels.Socket.Socket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MessageWriter {
    private final List<Message> writeQueue   = new ArrayList<>();
    private Message messageInProgress = null;

    private int bytesWritten=0;
    Socket socket;
    public void enqueue(Message message) {
        if(this.messageInProgress == null){
            this.messageInProgress = message;
        } else {
            this.writeQueue.add(message);
        }
    }

    public MessageWriter(Socket socket){
        this.socket = socket;
    }

    public void write(ByteBuffer byteBuffer) throws IOException {
        byteBuffer.put(messageInProgress.bytes);
        byteBuffer.flip();

        bytesWritten += socket.write(byteBuffer);
        byteBuffer.clear();

        if(bytesWritten >= this.messageInProgress.bytes.length){
            if(!this.writeQueue.isEmpty()){
                this.messageInProgress = this.writeQueue.remove(0);
            } else {
                this.messageInProgress = null;
            }
            bytesWritten = 0;
        }
    }

    public boolean isEmpty() {
        return this.writeQueue.isEmpty() && this.messageInProgress == null;
    }
}
