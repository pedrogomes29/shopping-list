package NIOChannels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MessageReader {

    List<Message> messages;
    Socket socket;

    byte[] lastMessage;

    public MessageReader(Socket socket){
        messages = new ArrayList<>();
        this.socket = socket;
        lastMessage = new byte[0];
    }

    public int findNextLineBreak(byte[] src,int starting_index) {
        byte[] lineBreak = "\n".getBytes();
        for(int index = starting_index; index < src.length; index++){
            int lineBreakIndex = lineBreak.length-1;
            if(src[index] == lineBreak[lineBreakIndex]){
                while(lineBreakIndex>=0){
                    if(index<0 || src[index] != lineBreak[lineBreakIndex])
                        return -1;
                    lineBreakIndex--;
                    index--;
                }
                return index;
            };
        }
        return -1;
    }

    public void read(ByteBuffer byteBuffer)  {
        byteBuffer.flip();
        byte[] src = new byte[lastMessage.length+byteBuffer.remaining()];
        System.arraycopy(lastMessage, 0, src, 0, lastMessage.length);
        byteBuffer.get(0,src,lastMessage.length,byteBuffer.remaining());
        int startIndex = 0;
        int endIndex = findNextLineBreak(src,startIndex);
        while(endIndex != -1) {
            messages.add(new Message(Arrays.copyOfRange(src, startIndex, endIndex + 1),this.socket));
            startIndex = endIndex + "\n".getBytes().length + 1;
            endIndex = findNextLineBreak(src,startIndex);
        }
        lastMessage = Arrays.copyOfRange(byteBuffer.array(), startIndex, byteBuffer.limit());
        byteBuffer.clear();
    }
}
