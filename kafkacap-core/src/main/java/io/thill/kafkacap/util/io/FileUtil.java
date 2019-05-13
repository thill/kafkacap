/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
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
