package Utils;

public class MessageBuilder {
    public static byte[] buildMessage(byte[] requestHeader, byte[] requestBody){
        byte[] lineSeparator = "\n".getBytes();
        byte[] messageBytes = new byte[requestHeader.length + requestBody.length + lineSeparator.length];

        System.arraycopy(requestHeader,0,messageBytes,0,requestHeader.length);
        System.arraycopy(requestBody,0,messageBytes,requestHeader.length,requestBody.length);
        System.arraycopy(lineSeparator,0,messageBytes,requestHeader.length+requestBody.length,lineSeparator.length);

        return messageBytes;
    }
}
