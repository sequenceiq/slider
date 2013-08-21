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

package org.apache.hadoop.hoya.tools;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hoya.HoyaKeys;
import org.apache.hadoop.hoya.api.ClusterDescription;
import org.apache.hadoop.hoya.exceptions.BadConfigException;
import org.apache.hadoop.hoya.exceptions.MissingArgException;
import org.apache.hadoop.hoya.providers.hbase.HBaseConfigFileOptions;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * These are hoya-specific Util methods
 */
public final class HoyaUtils {

  private static final Logger log = LoggerFactory.getLogger(HoyaUtils.class);

  private HoyaUtils() {
  }

  public static void deleteDirectoryTree(File dir) throws IOException {
    if (dir.exists()) {
      if (dir.isDirectory()) {
        log.info("Cleaning up {}", dir);
        //delete the children
        File[] files = dir.listFiles();
        if (files == null) {
          throw new IOException("listfiles() failed for " + dir);
        }
        for (File file : files) {
          log.info("deleting {}", file);
          file.delete();
        }
        dir.delete();
      } else {
        throw new IOException("Not a directory " + dir);
      }
    } else {
      //not found, do nothing
      log.debug("No output dir yet");
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
    ClassLoader loader = my_class.getClassLoader();
    if (loader == null) {
      throw new IOException(
        "Class " + my_class + " does not have a classloader!");
    }
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    Enumeration<URL> urlEnumeration = loader.getResources(class_file);
    if (urlEnumeration == null) {
      throw new IOException("Unable to find resources for class " + my_class);
    }

    for (Enumeration itr = urlEnumeration; itr.hasMoreElements(); ) {
      URL url = (URL) itr.nextElement();
      if ("jar".equals(url.getProtocol())) {
        String toReturn = url.getPath();
        if (toReturn.startsWith("file:")) {
          toReturn = toReturn.substring("file:".length());
        }
        // URLDecoder is a misnamed class, since it actually decodes
        // x-www-form-urlencoded MIME type rather than actual
        // URL encoding (which the file path has). Therefore it would
        // decode +s to ' 's which is incorrect (spaces are actually
        // either unencoded or encoded as "%20"). Replace +s first, so
        // that they are kept sacred during the decoding process.
        toReturn = toReturn.replaceAll("\\+", "%2B");
        toReturn = URLDecoder.decode(toReturn, "UTF-8");
        String jarFilePath = toReturn.replaceAll("!.*$", "");
        return new File(jarFilePath);
      } else {
        log.info("could not locate JAR containing {} URL={}", my_class, url);
      }
    }
    return null;
  }

  public static void checkPort(String hostname, int port, int connectTimeout)
    throws IOException {
    InetSocketAddress addr = new InetSocketAddress(hostname, port);
    checkPort(hostname, addr, connectTimeout);
  }

  @SuppressWarnings("SocketOpenedButNotSafelyClosed")
  public static void checkPort(String name,
                               InetSocketAddress address,
                               int connectTimeout)
    throws IOException {
    Socket socket = null;
    try {
      socket = new Socket();
      socket.connect(address, connectTimeout);
    } catch (Exception e) {
      throw new IOException("Failed to connect to " + name
                            + " at " + address
                            + " after " + connectTimeout + "millisconds"
                            + ": " + e,
                            e);
    } finally {
      IOUtils.closeQuietly(socket);
    }
  }

  public static void checkURL(String name, String url, int timeout) throws
                                                                    IOException {
    InetSocketAddress address = NetUtils.createSocketAddr(url);
    checkPort(name, address, timeout);
  }

  /**
   * A required file
   * @param role role of the file (for errors)
   * @param filename the filename
   * @throws ExitUtil.ExitException if the file is missing
   * @return the file
   */
  public static File requiredFile(String filename, String role) throws
                                                                IOException {
    if (filename.isEmpty()) {
      throw new ExitUtil.ExitException(-1, role + " file not defined");
    }
    File file = new File(filename);
    if (!file.exists()) {
      throw new ExitUtil.ExitException(-1,
                                       role + " file not found: " +
                                       file.getCanonicalPath());
    }
    return file;
  }

  /**
   * Normalize a cluster name then verify that it is valid
   * @param name proposed cluster name
   * @return true iff it is valid
   */
  public static boolean isClusternameValid(String name) {
    if (name == null || name.isEmpty()) {
      return false;
    }
    name = normalizeClusterName(name);
    int first = name.charAt(0);
    if (!Character.isLetter(first)) {
      return false;
    }

    for (int i = 0; i < name.length(); i++) {
      int elt = (int) name.charAt(i);
      if (!Character.isLetterOrDigit(elt) && elt != '-') {
        return false;
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
    return name.toLowerCase(Locale.ENGLISH);
  }

  /**
   * Copy a directory to a new FS -both paths must be qualified
   * @param conf conf file
   * @param srcDirPath src dir
   * @param destDirPath dest dir
   * @return # of files copies
   */
  public static int copyDirectory(Configuration conf,
                                  Path srcDirPath,
                                  Path destDirPath) throws IOException {
    FileSystem srcFS = FileSystem.get(srcDirPath.toUri(), conf);
    FileSystem destFS = FileSystem.get(destDirPath.toUri(), conf);
    //list all paths in the src.
    FileStatus[] entries = srcFS.listStatus(srcDirPath);
    int srcFileCount = entries.length;
    if (srcFileCount == 0) {
      return 0;
    }
    if (!destFS.exists(destDirPath)) {
      destFS.mkdirs(destDirPath);
    }
    Path[] sourcePaths = new Path[srcFileCount];
    for (int i = 0; i < srcFileCount; i++) {
      FileStatus e = entries[i];
      Path srcFile = e.getPath();
      if (srcFS.isDirectory(srcFile)) {
        throw new IOException("Configuration dir " + srcDirPath
                              + " contains a directory " + srcFile);
      }
      log.debug("copying src conf file {}", srcFile);
      sourcePaths[i] = srcFile;
    }
    log.debug("Copying {} files to dest dir {}", srcFileCount, destDirPath);
    FileUtil.copy(srcFS, sourcePaths, destFS, destDirPath, false, true, conf);
    return srcFileCount;
  }

  /**
   * Create the Hoya cluster path for a named cluster.
   * This is a directory; a mkdirs() operation is executed
   * to ensure that it is there.
   * @param fs filesystem
   * @param clustername name of the cluster
   * @return the path for persistent data
   */
  public static Path createHoyaClusterDirPath(FileSystem fs,
                                              String clustername) throws
                                                                  IOException {
    Path hoyaPath = getBaseHoyaPath(fs);
    Path instancePath = new Path(hoyaPath, "cluster/" + clustername);
    fs.mkdirs(instancePath);
    return instancePath;
  }

  /**
   * Create the application-instance specific temporary directory
   * in the DFS
   * @param fs filesystem
   * @param clustername name of the cluster
   * @param appID appliation ID
   * @return the path; this directory will already have been created
   */
  public static Path createHoyaAppInstanceTempPath(FileSystem fs,
                                                   String clustername,
                                                   String appID) throws
                                                                 IOException {
    Path hoyaPath = getBaseHoyaPath(fs);
    Path instancePath = new Path(hoyaPath, "tmp/" + clustername + "/" + appID);
    fs.mkdirs(instancePath);
    return instancePath;
  }

  /**
   * Get the base path for hoya
   * @param fs
   * @return
   */
  public static Path getBaseHoyaPath(FileSystem fs) {
    return new Path(fs.getHomeDirectory(), ".hoya");
  }

  public static String stringify(Throwable t) {
    StringWriter sw = new StringWriter();
    sw.append(t.toString()).append('\n');
    t.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }

  /**
   * Create a configuration with Hoya-specific tuning.
   * This is done rather than doing custom configs.
   * @return the config
   */
  public static YarnConfiguration createConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    patchConfiguration(conf);
    return conf;
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
    if (conf.get(HBaseConfigFileOptions.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH) == null) {
      conf.set(HBaseConfigFileOptions.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH, "true");
    }
  }

  /**
   * Overwrite a cluster specification. This code
   * attempts to do this atomically by writing the updated specification
   * to a new file, renaming the original and then updating the original.
   * There's special handling for one case: the original file doesn't exist
   * @param clusterFS
   * @param clusterSpec
   * @param clusterDirectory
   * @param clusterSpecPath
   * @return true if the original cluster specification was updated.
   */
  public static boolean updateClusterSpecification(FileSystem clusterFS,
                                                   Path clusterDirectory,
                                                   Path clusterSpecPath,
                                                   ClusterDescription clusterSpec) throws
                                                                                   IOException {

    //it is not currently there -do a write with overwrite disabled, so that if
    //it appears at this point this is picked up
    if (!clusterFS.exists(clusterSpecPath) &&
        writeSpecWithoutOverwriting(clusterFS, clusterSpecPath, clusterSpec)) {
      return true;
    }

    //save to a renamed version
    String specTimestampedFilename = "spec-" + System.currentTimeMillis();
    Path specSavePath =
      new Path(clusterDirectory, specTimestampedFilename + ".json");
    Path specOrigPath =
      new Path(clusterDirectory, specTimestampedFilename + "-orig.json");

    //roll the specification. The (atomic) rename may fail if there is 
    //an overwrite, which is how we catch re-entrant calls to this
    if (!writeSpecWithoutOverwriting(clusterFS, specSavePath, clusterSpec)) {
      return false;
    }
    if (!clusterFS.rename(clusterSpecPath, specOrigPath)) {
      return false;
    }
    try {
      if (!clusterFS.rename(specSavePath, clusterSpecPath)) {
        return false;
      }
    } finally {
      clusterFS.delete(specOrigPath, false);
    }
    return true;
  }

  public static boolean writeSpecWithoutOverwriting(FileSystem clusterFS,
                                                    Path clusterSpecPath,
                                                    ClusterDescription clusterSpec) {
    try {
      clusterSpec.save(clusterFS, clusterSpecPath, false);
    } catch (IOException e) {
      log.debug("Failed to save cluster specification -race condition? " + e,
                e);
      return false;
    }
    return true;
  }

  public static boolean maybeAddImagePath(FileSystem clusterFS,
                                          Map<String, LocalResource> localResources,
                                          Path imagePath) throws IOException {
    if (imagePath != null) {
      LocalResource resource = createAmResource(clusterFS,
                                                imagePath,
                                                LocalResourceType.ARCHIVE);
      localResources.put(HoyaKeys.HBASE_LOCAL, resource);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Take a collection, return a list containing the string value of every
   * element in the collection.
   * @param c collection
   * @return a stringified list
   */
  public static List<String> collectionToStringList(Collection c) {
    List<String> l = new ArrayList<String>(c.size());
    for (Object o : c) {
      l.add(o.toString());
    }
    return l;
  }

  public static String join(Collection collection, String separator) {
    StringBuilder b = new StringBuilder();
    for (Object o : collection) {
      b.append(o.toString());
      b.append(separator);
    }
    return b.toString();
  }

  public static String join(String[] collection, String separator) {
    StringBuilder b = new StringBuilder();
    for (String o : collection) {
      b.append(o);
      b.append(separator);
    }
    return b.toString();
  }

  public static String mandatoryEnvVariable(String key) {
    String v = System.getenv(key);
    if (v == null) {
      throw new MissingArgException("Missing Environment variable " + key);
    }
    return v;
  }
  
  public static String appReportToString(ApplicationReport r, String separator) {
    StringBuilder builder = new StringBuilder(512);
    builder.append("application ").append(
      r.getName()).append("/").append(r.getApplicationType());
    builder.append(separator).append(
      "state: ").append(r.getYarnApplicationState());
    builder.append(separator).append("URL: ").append(r.getTrackingUrl());
    builder.append(separator).append("Started ").append(new Date(r.getStartTime()).toLocaleString());
    long finishTime = r.getFinishTime();
    if (finishTime>0) {
      builder.append(separator).append("Finished ").append(new Date(finishTime).toLocaleString());
    }
    builder.append(separator).append("RPC :").append(r.getHost()).append(':').append(r.getRpcPort());
    String diagnostics = r.getDiagnostics();
    if (!diagnostics.isEmpty()) {
      builder.append(separator).append("Diagnostics :").append(diagnostics);
    }
    return builder.toString();
  }

  
  public static Map<String, String>  mergeMap(Map<String, String> dest,
           Map<String, String> src) {
    for (Map.Entry<String, String> entry: src.entrySet()) {
      dest.put(entry.getKey(), entry.getValue());
    }
    return dest;
  }

  public static String stringifyMap(Map<String, String> map) {
    StringBuilder builder =new StringBuilder();
    for (Map.Entry<String, String> entry: map.entrySet()) {
      builder.append(entry.getKey())
             .append("=\"")
             .append(entry.getValue())
             .append("\"\n");
      
    }
    return builder.toString();
  }


  /**
   * Get the int value of a role
   * @param roleMap map of role key->val entries
   * @param key key the key to look for
   * @param defVal default value to use if the key is not in the map
   * @param min min value or -1 for do not check
   * @param max max value or -1 for do not check
   * @return the int value the integer value
   * @throws BadConfigException if the value could not be parsed
   */
  public static int getIntValue(Map<String, String> roleMap,
                         String key,
                         int defVal,
                         int min,
                         int max
                        ) throws BadConfigException {
    String valS = roleMap.get(key);
    return parseAndValidate(key, valS, defVal, min, max);

  }

  /**
   * Parse an int value, replacing it with defval if undefined;
   * @param errorKey key to use in exceptions
   * @param defVal default value to use if the key is not in the map
   * @param min min value or -1 for do not check
   * @param max max value or -1 for do not check
   * @return the int value the integer value
   * @throws BadConfigException if the value could not be parsed
   */
  public static int parseAndValidate(String errorKey,
                                     String valS,
                                     int defVal,
                                     int min, int max) throws
                                                       BadConfigException {
    if (valS == null) {
      valS = Integer.toString(defVal);
    }
    String trim = valS.trim();
    int val;
    try {
      val = Integer.decode(trim);
    } catch (NumberFormatException e) {
      throw new BadConfigException("Failed to parse value of "
                                   + errorKey + ": \"" + trim + "\"");
    }
    if (min>=0 && val<min) {
      throw new BadConfigException("Value of "
                                   + errorKey + ": " + val+ "" 
      + "is less than the minimum of " + min);

    }
    if (max>=0 && val>max) {
      throw new BadConfigException("Value of "
                                   + errorKey + ": " + val+ "" 
      + "is more than the maximum of " + max);

    }
    return val;
  }

  /**
   * Create an AM resource from the 
   * @param hdfs HDFS or other filesystem in use
   * @param destPath dest path in filesystem
   * @param resourceType resource type
   * @return the resource set up wih application-level visibility and the
   * timestamp & size set from the file stats.
   */
  public static LocalResource createAmResource(FileSystem hdfs,
                                               Path destPath,
                                               LocalResourceType resourceType) throws
                                                                               IOException {
    FileStatus destStatus = hdfs.getFileStatus(destPath);
    LocalResource amResource = Records.newRecord(LocalResource.class);
    amResource.setType(resourceType);
    // Set visibility of the resource 
    // Setting to most private option
    amResource.setVisibility(LocalResourceVisibility.APPLICATION);
    // Set the resource to be copied over
    amResource.setResource(ConverterUtils.getYarnUrlFromPath(destPath));
    // Set timestamp and length of file so that the framework 
    // can do basic sanity checks for the local resource 
    // after it has been copied over to ensure it is the same 
    // resource the client intended to use with the application
    amResource.setTimestamp(destStatus.getModificationTime());
    amResource.setSize(destStatus.getLen());
    return amResource;
  }

  public static InetSocketAddress getRmAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_PORT);
  }

  public static InetSocketAddress getRmSchedulerAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
  }

  /**
   * probe to see if the RM scheduler is defined
   * @param conf config
   * @return true if the RM scheduler address is set to
   * something other than 0.0.0.0
   */
  public static boolean isRmSchedulerAddressDefined(Configuration conf) {
    InetSocketAddress address = getRmSchedulerAddress(conf);
    return isAddressDefined(address);
  }

  /**
   * probe to see if the address
   * @param address
   * @return true if the scheduler address is set to
   * something other than 0.0.0.0
   */
  public static boolean isAddressDefined(InetSocketAddress address) {
    return !(address.getHostName().equals("0.0.0.0"));
  }

  public static void setRmAddress(Configuration conf, String rmAddr) {
    conf.set(YarnConfiguration.RM_ADDRESS, rmAddr);
  }

  public static void setRmSchedulerAddress(Configuration conf, String rmAddr) {
    conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, rmAddr);
  }

  public static boolean hasAppFinished(ApplicationReport report) {
    return report == null ||
           report.getYarnApplicationState().ordinal() >=
           YarnApplicationState.FINISHED.ordinal();
  }

  public static String reportToString(ApplicationReport report) {
    if (report == null) {
      return "Null application report";
    }

    return "App " + report.getName() + "/" + report.getApplicationType() +
           "# " +
           report.getApplicationId() + " user " + report.getUser() +
           " is in state " + report.getYarnApplicationState() +
           "RPC: " + report.getHost() + ":" + report.getRpcPort();
  }

  /**
   * Register all files under a fs path as a directory to push out 
   * @param clusterFS cluster filesystem
   * @param srcDir src dir
   * @param destRelativeDir dest dir (no trailing /)
   * @return the list of entries
   */
  public static Map<String, LocalResource> submitDirectory(FileSystem clusterFS,
                                                           Path srcDir,
                                                           String destRelativeDir) throws
                                                                                   IOException {
    //now register each of the files in the directory to be
    //copied to the destination
    FileStatus[] fileset = clusterFS.listStatus(srcDir);
    Map<String, LocalResource> localResources =
      new HashMap<String, LocalResource>(fileset.length);
    for (FileStatus entry : fileset) {

      LocalResource resource = createAmResource(clusterFS,
                                                entry.getPath(),
                                                LocalResourceType.FILE);
      String relativePath = destRelativeDir + "/" + entry.getPath().getName();
      localResources.put(relativePath, resource);
    }
    return localResources;
  }

  public static String stringify(org.apache.hadoop.yarn.api.records.URL url) {
    return url.getScheme() + ":/" +
           (url.getHost() != null ? url.getHost() : "") + "/" + url.getFile();
  }

  public static int findFreePort(int start, int limit) {
    int found = 0;
    int port = start;
    int finish = start + limit;
    while (found == 0 && port < finish) {
      if (isPortAvailable(port)) {
        found = port;
      } else {
        port++;
      }
    }
    return found;
  }

  /**
   * See if a port is available for listening on by trying to listen
   * on it and seeing if that works or fails.
   * @param port port to listen to
   * @return true if the port was available for listening on
   */
  public static boolean isPortAvailable(int port) {
    try {
      ServerSocket socket = new ServerSocket(port);
      socket.close();
      return true;
    } catch (IOException e) {
      return false;
    }
  }


  /**
   * This wrapps ApplicationReports and generates a string version
   * iff the toString() operator is invoked
   */
  public static class OnDemandReportStringifier {
    private final ApplicationReport report;

    public OnDemandReportStringifier(ApplicationReport report) {
      this.report = report;
    }

    @Override
    public String toString() {
      return appReportToString(report, "\n");
    }
  }
  
}
