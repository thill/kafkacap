package io.thill.kafkacap.util.io;

import java.io.*;

public class ResourceLoader {

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
