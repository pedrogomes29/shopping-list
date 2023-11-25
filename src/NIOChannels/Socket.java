package NIOChannels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Socket {
    private long socketId;
    public boolean endOfStreamReached;
    public SocketChannel socketChannel;
    public MessageReader messageReader;
    public MessageWriter  messageWriter;

    public Socket(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        this.messageReader = new MessageReader(this);
        this.messageWriter = new MessageWriter(this);
        this.endOfStreamReached = false;
    }

    public Socket(long socketId){
        this.socketId = socketId;
    }
    public int read(ByteBuffer byteBuffer) throws IOException {
        int bytesRead = this.socketChannel.read(byteBuffer);
        int totalBytesRead = bytesRead;

        while(bytesRead > 0){
            bytesRead = this.socketChannel.read(byteBuffer);
            totalBytesRead += bytesRead;
        }
        if(bytesRead == -1){
            this.endOfStreamReached = true;
        }

        return totalBytesRead;
    }

    public int write(ByteBuffer byteBuffer) throws IOException {
        int bytesWritten      = this.socketChannel.write(byteBuffer);
        int totalBytesWritten = bytesWritten;

        while(bytesWritten > 0 && byteBuffer.hasRemaining()){
            bytesWritten = this.socketChannel.write(byteBuffer);
            totalBytesWritten += bytesWritten;
        }

        return totalBytesWritten;
    }


    public long getSocketId(){
        return this.socketId;
    }

    public void setSocketId(long socketId) {
        this.socketId = socketId;
    }
}
