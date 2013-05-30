/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hoya.tools

import groovy.util.logging.Commons
import org.apache.commons.io.IOUtils
import org.apache.commons.logging.Log
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.util.ExitUtil.ExitException

/**
 * Utility methods primarily used in setting up and executing tools
 */
@Commons
class HoyaUtils {

  static void dumpArguments(String[] args) {
    println("Arguments");
    println(convertArgsToString(args));
  }

  static void dumpConf(Configuration conf) {
    TreeSet<String> keys = sortedConfigKeys(conf);
    keys.each { key ->
      println("$key = ${conf.get(key)}")
    }
  }

  public static String convertArgsToString(String... args) {
    StringBuilder builder = new StringBuilder();
    args.each { arg ->
      builder.append(" \"").append(arg).append("\"");
    }
    builder.toString();
  }

  static long dumpDir(Log dumpLog, File dir, String pattern) {
    if (!dir.exists()) {
      dumpLog.warn("Not found: ${dir}");
      return -1;
    }
    if (!dir.isDirectory()) {
      dumpLog.warn("Not a directory: ${dir}");
      return -1;
    }
    int size = 0;
    dir.eachFile { file ->
      long l = dumpFile(dumpLog, file)
      if (file.name.startsWith(pattern)) {
        size += l
      }
    }
    size;
  }

  static long dumpFile(Log dumpLog, File file) {
    long length = file.length()
    dumpLog.info("File : ${file} of size ${length}")
    length
  }

  static String convertToUrl(File file) {
    return file.toURI().toString();
  }

  static def deleteDirectoryTree(File dir) {
    if (dir.exists()) {
      if (dir.isDirectory()) {
        log.info("Cleaning up $dir")
        //delete the children
        dir.eachFile { file ->
          log.info("deleting $file")
          file.delete()
        }
        dir.delete()
      } else {
        throw new IOException("Not a directory: ${dir}")
      }
    } else {
      //not found, do nothing
      log.debug("No output dir yet")
    }
  }

  /**
   * Find a containing JAR
   * @param my_class class to find
   * @return the file or null if it is not found
   * @throws IOException any IO problem, including the class not having a 
   * classloader
   */
  public static File findContainingJar(Class my_class) throws IOException {
    ClassLoader loader = my_class.classLoader;
    if (!loader) {
      throw new IOException("Class $my_class does not have a classloader!")
    }
    assert loader != null
    assert my_class != null
    String class_file = my_class.name.replaceAll("\\.", "/") + ".class";
    Enumeration<URL> urlEnumeration = loader.getResources(class_file)
    assert urlEnumeration != null

    for (Enumeration itr = urlEnumeration; itr.hasMoreElements();) {
      URL url = (URL) itr.nextElement()
      if ("jar".equals(url.protocol)) {
        String toReturn = url.path
        if (toReturn.startsWith("file:")) {
          toReturn = toReturn.substring("file:".length())
        }
        // URLDecoder is a misnamed class, since it actually decodes
        // x-www-form-urlencoded MIME type rather than actual
        // URL encoding (which the file path has). Therefore it would
        // decode +s to ' 's which is incorrect (spaces are actually
        // either unencoded or encoded as "%20"). Replace +s first, so
        // that they are kept sacred during the decoding process.
        toReturn = toReturn.replaceAll("\\+", "%2B");
        toReturn = URLDecoder.decode(toReturn, "UTF-8");
        String jarFilePath = toReturn.replaceAll("!.*\$", "")
        return new File(jarFilePath)
      } else {
        log.info("could not locate JAR containing $my_class: URL=$url")
      }
    }
    return null;
  }

  public static void checkPort(String hostname, int port, int connectTimeout)
  throws IOException {
    InetSocketAddress addr = new InetSocketAddress(hostname, port);
    checkPort(hostname, addr, connectTimeout)
  }

  public static void checkPort(String name, InetSocketAddress address, int connectTimeout)
  throws IOException {
    Socket socket = null;
    try {
      socket = new Socket();
      socket.connect(address, connectTimeout);
    } catch (Exception e) {
      throw new IOException("Failed to connect to $name at $address"
                                + " after $connectTimeout millisconds"
                                + ": " + e).initCause(e);
    } finally {
      IOUtils.closeQuietly(socket)
    }
  }

  public static void checkURL(String name, String url, int timeout) {
    InetSocketAddress address = NetUtils.createSocketAddr(url)
    checkPort(name, address, timeout)
  }

  public static TreeSet<String> sortedConfigKeys(Configuration conf) {
    TreeSet<String> sorted = new TreeSet<String>();
    conf.each { entry ->
      sorted.add(entry.key)
    }
    sorted;
  }

  /**
   * A required file
   * @param role role of the file (for errors)
   * @param filename the filename
   * @throws ExitException if the file is missing
   * @return the file
   */
  public static File requiredFile(String filename, String role) {
    if (!filename) {
      throw new ExitException(-1, "$role file not defined");
    }
    File file = new File(filename)
    if (!file.exists()) {
      throw new ExitException(-1, "$role file not found: \"${file.canonicalPath}\"");
    }
    file
  }

  protected static File requiredDir(String name) {
    File dir = requiredFile(name, "")
    if (!dir.directory) {
      throw new ExitException(-1, "Not a directory: " + dir.canonicalPath)
    }
    dir
  }

  protected static File maybeCreateDir(String name) {
    File dir = new File(name)
    if (!dir.exists()) {
      //this is what we want
      if (!dir.mkdirs()) {
        throw new ExitException(-1, "Failed to create directory " + dir.canonicalPath)
      }
    } else {
      if (!dir.directory) {
        throw new ExitException(-1, "Not a directory: " + dir.canonicalPath)
      }
    }
    dir
  }

}
