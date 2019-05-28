/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.core.util.io;

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
    longToBytes(l, result, 0);
    return result;
  }

  /**
   * Convert a long to a byte array. Big Endian.
   *
   * @param l      The long value
   * @param buffer The output buffer
   * @param offset The output buffer offset
   * @return The number of bytes written.  Always 8.
   */
  public static int longToBytes(long l, byte[] buffer, int offset) {
    for(int i = 7; i >= 0; i--) {
      buffer[offset + i] = (byte)(l & 0xFF);
      l >>= 8;
    }
    return 8;
  }

  /**
   * Convert a byte array to a long. Big Endian.
   *
   * @param b The byte array
   * @return The long value
   */
  public static long bytesToLong(byte[] b) {
    return bytesToLong(b, 0);
  }

  /**
   * Convert a byte array to a long from the given offset. Big Endian.
   *
   * @param buffer The byte array
   * @param offset The offset in the buffer
   * @return The long value
   */
  public static long bytesToLong(byte[] buffer, int offset) {
    long result = 0;
    for(int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (buffer[offset + i] & 0xFF);
    }
    return result;
  }
}
