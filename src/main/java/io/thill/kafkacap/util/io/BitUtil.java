package io.thill.kafkacap.util.io;

public class BitUtil {
  public static byte[] longToBytes(long l) {
    byte[] result = new byte[8];
    for(int i = 7; i >= 0; i--) {
      result[i] = (byte)(l & 0xFF);
      l >>= 8;
    }
    return result;
  }

  public static long bytesToLong(byte[] b) {
    long result = 0;
    for(int i = 0; i < b.length; i++) {
      result <<= 8;
      result |= (b[i] & 0xFF);
    }
    return result;
  }
}
