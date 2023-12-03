package Utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hasher {
    public static String md5(String textToHash){
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            // this should never happen
            throw new RuntimeException(e);
        }
        byte[] bytes = md.digest(textToHash.getBytes());
        return bytesToHex(bytes);
    }

    private static String bytesToHex(byte[] bytes){
        StringBuilder hex = new StringBuilder();
        for (byte aByte : bytes) {
            hex.append(Integer.toString((aByte & 0xff) + 0x100, 16)
                    .substring(1));
        }
        return hex.toString();
    }

}
