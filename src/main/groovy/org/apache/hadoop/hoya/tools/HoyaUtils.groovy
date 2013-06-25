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

import groovy.transform.CompileStatic
import groovy.util.logging.Commons
import org.apache.commons.io.IOUtils
import org.apache.commons.logging.Log
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hoya.exceptions.HoyaException
import org.apache.hadoop.hoya.yarn.appmaster.EnvMappings
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.util.ExitUtil.ExitException

/**
 * Utility methods primarily used in setting up and executing tools
 */
@Commons
@CompileStatic

/**
 * This contains general utility methods -including some that
 * are HoyaSpecific
 */
class HoyaUtils {


  public static void dumpArguments(String[] args) {
    println("Arguments");
    println(convertArgsToString(args));
  }


  /**
   * Quote any argument passed in that doesn't start with a single or double quote
   * in the first place
   * @param arg argument, may be null but not empty
   * @return a quoted argument
   */
  public static String quoteArg(String arg) {
    if (arg.length() == 0) {
      return "";
    }
    if (arg[0]=='"' || arg[0] == '\'') {
      return arg;
    } else {
      return '"' + arg + '"'
    }
  }

  /**
   * convert a file to a string and quote it
   * @param f file
   * @return a quoted filename
   */
  public static String quoteArg(File f) {
    return quoteArg(f.absolutePath)
  }

  public static String convertArgsToString(String... args) {
    StringBuilder builder = new StringBuilder();
    args.each { arg ->
      builder.append(" \"").append(arg).append("\"");
    }
    builder.toString();
  }

  public static long dumpDir(Log dumpLog, File dir, String pattern) {
    if (!dir.exists()) {
      dumpLog.warn("Not found: ${dir}");
      return -1;
    }
    if (!dir.isDirectory()) {
      dumpLog.warn("Not a directory: ${dir}");
      return -1;
    }
    long size = 0;
    dir.eachFile { File file ->
      long l = dumpFile(dumpLog, file)
      if (file.name.startsWith(pattern)) {
        size += l
      }
    }
    size;
  }

  public static long dumpFile(Log dumpLog, File file) {
    long length = file.length()
    dumpLog.info("File : ${file} of size ${length}")
    length
  }

  public static String convertToUrl(File file) {
    return file.toURI().toString();
  }

  public static def deleteDirectoryTree(File dir) {
    if (dir.exists()) {
      if (dir.isDirectory()) {
        log.info("Cleaning up $dir")
        //delete the children
        dir.eachFile { File file ->
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
      throw new IOException("Failed to connect to $name at $address" +
                            " after $connectTimeout millisconds" +
                            ": $e", e);
    } finally {
      IOUtils.closeQuietly(socket)
    }
  }

  public static void checkURL(String name, String url, int timeout) {
    InetSocketAddress address = NetUtils.createSocketAddr(url)
    checkPort(name, address, timeout)
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
    return dir
  }

  /**
   * Normalize a cluster name then verify that it is valid
   * @param name proposed cluster name
   * @return true iff it is valid
   */
  public static boolean isClusternameValid(String name) {
    if (!name) {
      return false
    };
    name = normalizeClusterName(name)
    int first = name.charAt(0)
    if (!Character.isLetter(first)) {
      return false;
    }

    for (int i = 0; i < name.length(); i++) {
      int elt = (int) name.charAt(i)
      if (!Character.isLetterOrDigit(elt) && elt != '-') {
        return false
      } 
    }
    return true;
  }

  /**
   * Perform whatever operations are needed to make different
   * case cluster names consistent
   * @param name cluster name
   * @return the normalized one (currently: the lower case name)
   */
  public static String normalizeClusterName(String name) {
    return name.toLowerCase(Locale.ENGLISH)
  }

  /**
   * Copy a directory to a new FS -both paths must be qualified
   * @param conf conf file
   * @param srcDirPath src dir
   * @param destDirPath dest dir
   * @return #of files copies
   */
  public static int copyDirectory(Configuration conf, Path srcDirPath, Path destDirPath) {
    HadoopFS srcFS = HadoopFS.get(srcDirPath.toUri(), conf)
    HadoopFS destFS = HadoopFS.get(destDirPath.toUri(), conf)
    //list all paths in the src.
    FileStatus[] entries = srcFS.listStatus(srcDirPath)
    int srcFileCount = entries.size()
    if (!srcFileCount) {
      return 0;
    }
    if (!destFS.exists(destDirPath)) {
      destFS.mkdirs(destDirPath);
    }
    Path[] sourcePaths = new Path[srcFileCount]
    entries.eachWithIndex { FileStatus e , int i ->
      Path srcFile = e.path
      if (srcFS.isDirectory(srcFile)) {
        throw new HoyaException("Configuration dir $srcDirPath contains a directory");
      }
      log.debug("copying src conf file $srcFile")
      sourcePaths[i] = srcFile
    }
    log.debug("Copying $srcFileCount files to dest dir $destDirPath")
    FileUtil.copy(srcFS, sourcePaths, destFS, destDirPath, false, true, conf)
    return sourcePaths.size();
  }

  /**
   * Create the Hoya cluster path for a named cluster.
   * This is a directory; a mkdirs() operation is executed
   * to ensure that it is there.
   * @param fs filesystem
   * @param clustername name of the cluster
   * @return the path for persistent data
   */
  public static Path createHoyaClusterDirPath(HadoopFS fs, String clustername) {
    Path hoyaPath = getBaseHoyaPath(fs);
    Path instancePath = new Path(hoyaPath, "cluster/$clustername")
    fs.mkdirs(instancePath);
    return instancePath
  }

  /**
   * Create the application-instance specific temporary directory
   * in the DFS
   * @param fs filesystem
   * @param clustername name of the cluster
   * @param appID appliation ID
   * @return the path; this directory will already have been created
   */
  public static Path createHoyaAppInstanceTempPath(HadoopFS fs,
                                            String clustername,
                                            String appID){
    Path hoyaPath = getBaseHoyaPath(fs);
    Path instancePath = new Path(hoyaPath, "tmp/$clustername/$appID")
    fs.mkdirs(instancePath);
    return instancePath
  }

  /**
   * Get the base path for hoya
   * @param fs
   * @return
   */
  public static Path getBaseHoyaPath(HadoopFS fs) {
    return new Path(fs.homeDirectory, ".hoya")
  }
  
  public static String stringify(Throwable t) {
    StringWriter sw = new StringWriter()
    sw.append(t.toString()).append('\n')
    t.printStackTrace(new PrintWriter(sw))
    return sw.toString()
  }

  /**
   * Create a configuration with Hoya-specific tuning.
   * This is done rather than doing custom configs.
   * @return
   */
  public static Configuration createConfiguration() {
    Configuration conf = new Configuration()
    patchConfiguration(conf)
    return conf
  }

  /**
   * Take an existing conf and patch it for Hoya's needs. Useful
   * in Service.init methods where a shared config is being
   * passed in
   * @param conf configuration
   */
  public static void patchConfiguration(Configuration conf) {
    
    //if the fallback option is NOT set, enable it.
    //if it is explicitly set to anything -leave alone
    if (conf.get(EnvMappings.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH) == null) {
      conf.set(EnvMappings.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH, "true")
    }
  }
}
