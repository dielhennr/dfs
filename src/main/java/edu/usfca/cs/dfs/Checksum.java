package edu.usfca.cs.dfs; 
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

/**
 * Computes the sha1sum of an array of bytes and formats the sum as hex digits 
 * this gives us 40 character hashes that we can append to each chunk's filename.
 *
 * Functions taken from some stack overflow page can't find it again 
 */
public class Checksum {

    /**
     * Computes SHA1SUM which returns 160 bytes
     * @param convertme
     * @return 40 character String representation of the sha1sum
     * @throws NoSuchAlgorithmException
     */
    public static String SHAsum(byte[] convertme) throws NoSuchAlgorithmException{
        MessageDigest md = MessageDigest.getInstance("SHA-1"); 
        return byteArray2Hex(md.digest(convertme));
    }
    
    /**
     * Returns the hex string of an array of 160 bytes from sha1sum 
     *
     * @param hash
     * @return hex character string representation of the input
     */
    private static String byteArray2Hex(final byte[] hash) {
        Formatter formatter = new Formatter();
        for (byte b : hash) {
            formatter.format("%02x", b);
        }
        String outputHash = formatter.toString();
        formatter.close();
        return outputHash;
    }
}
