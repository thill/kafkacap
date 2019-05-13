/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.util.io;

import java.io.*;

/**
 * Utility methods for dealing with resources
 *
 * @author Eric Thill
 */
public class ResourceLoader {

  /**
   * Open an {@link InputStream} for the given resource or file. Class resource is tried first, then system resource, then file.
   *
   * @param resourceOrFile The resource or file to open
   * @return The opened input stream
   * @throws FileNotFoundException
   */
  public static InputStream openResourceOrFile(String resourceOrFile) throws FileNotFoundException {
    InputStream in;

    in = ResourceLoader.class.getResourceAsStream(resourceOrFile);
    if(in != null)
      return in;

    in = ClassLoader.getSystemResourceAsStream(resourceOrFile);
    if(in != null)
      return in;

    return new FileInputStream(resourceOrFile);
  }

  /**
   * Load the given resource of file as a string. Class resource is tried first, then system resource, then file.
   *
   * @param resourceOrFile The resource or file to load
   * @return The resource or file's content as a string
   * @throws IOException
   */
  public static String loadResourceOrFile(String resourceOrFile) throws IOException {
    StringBuilder sb = new StringBuilder();
    try(BufferedReader br = new BufferedReader(new InputStreamReader(openResourceOrFile(resourceOrFile)))) {
      String line;
      while((line = br.readLine()) != null) {
        sb.append(line).append('\n');
      }
    }
    return sb.toString();
  }

}
