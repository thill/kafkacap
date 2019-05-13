/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.util.io;

/**
 * Utility functions for operating on byte arrays
 *
 * @author Eric Thill
 */
public class BitUtil {
  /**
   * Convert a long to a byte array. Big Endian.
   *
   * @param l The long value
   * @return The byte array
   */
  public static byte[] longToBytes(long l) {
    byte[] result = new byte[8];
    for(int i = 7; i >= 0; i--) {
      result[i] = (byte)(l & 0xFF);
      l >>= 8;
    }
    return result;
  }

  /**
   * Convert a byte arrat to a long. Big Endian.
   *
   * @param b The byte array
   * @return The long value
   */
  public static long bytesToLong(byte[] b) {
    long result = 0;
    for(int i = 0; i < b.length; i++) {
      result <<= 8;
      result |= (b[i] & 0xFF);
    }
    return result;
  }
}
