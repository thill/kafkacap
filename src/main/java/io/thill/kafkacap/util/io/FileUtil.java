package io.thill.kafkacap.util.io;

import java.io.File;

public class FileUtil {

  public static void deleteRecursive(File f) {
    if(f.isDirectory()) {
      for(File child : f.listFiles()) {
        deleteRecursive(child);
      }
    }
    f.delete();
  }

}
