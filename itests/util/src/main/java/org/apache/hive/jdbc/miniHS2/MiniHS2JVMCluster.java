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

package org.apache.hive.jdbc.miniHS2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.SettableFuture;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapItUtils;
import org.apache.hadoop.hive.llap.daemon.MiniLlapCluster;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.shims.HadoopShims.MiniDFSShim;
import org.apache.hadoop.hive.shims.HadoopShims.MiniMrShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.http.security.PamAuthenticator;
import org.apache.hive.jdbc.Utils;
import org.apache.hive.service.Service;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.hive.service.cli.thrift.ThriftHttpCLIService;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-node MiniHS2 cluster that shares resources (HDFS, MapReduce, Tez, etc.) 
 * across multiple HS2 instances for redundancy and load balancing.
 * 
 * This class maintains compatibility with existing MiniHS2 tests by providing
 * the same public interface while internally managing multiple HS2 instances.
 */
public class MiniHS2JVMCluster extends AbstractHiveService {

  private static final Logger LOG = LoggerFactory.getLogger(MiniHS2JVMCluster.class);

  public static final String HS2_BINARY_MODE = "binary";
  public static final String HS2_HTTP_MODE = "http";
  public static final String HS2_ALL_MODE = "all";
  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  private static final FsPermission FULL_PERM = new FsPermission((short)00777);
  private static final FsPermission WRITE_ALL_PERM = new FsPermission((short)00733);
  private static final String tmpDir = System.getProperty("test.tmp.dir");
  private static final int DEFAULT_DATANODE_COUNT = 4;
  private static final int DEFAULT_HS2_INSTANCES = 3;

  // Note: hiveServer2 is not used in cluster mode since we manage multiple instances
  // private HiveServer2 hiveServer2 = null;

  // Shared resources across all HS2 instances
  private final File baseDir;
  private final Path baseFsDir;
  private MiniMrShim mr;
  private MiniDFSShim dfs;
  private MiniLlapCluster llapCluster = null;
  private final FileSystem localFS;
  private boolean useMiniKdc = false;
  private final String serverPrincipal;
  private final boolean isMetastoreRemote;
  private final boolean cleanupLocalDirOnStartup;
  private final boolean isMetastoreSecure;
  private MiniClusterType miniClusterType = MiniClusterType.LOCALFS_ONLY;
  private boolean usePortsFromConf = false;
  private PamAuthenticator pamAuthenticator;
  private boolean createTransactionalTables;
  private int hmsPort = 0;

  // Multiple HS2 instances
  private final List<MiniHS2> hs2Instances = new ArrayList<>();
  private final int numInstances;
  private int currentInstanceIndex = 0; // For round-robin access

  public enum MiniClusterType {
    MR,
    TEZ,
    LLAP,
    LOCALFS_ONLY;
  }

  public static class Builder {
    private HiveConf hiveConf = new HiveConf();
    private MiniClusterType miniClusterType = MiniClusterType.LOCALFS_ONLY;
    private boolean useMiniKdc = false;
    private String serverPrincipal;
    private String serverKeytab;
    private boolean isHTTPTransMode = false;
    private boolean isMetastoreRemote;
    private boolean usePortsFromConf = false;
    private String authType = "KERBEROS";
    private boolean isHA = false;
    private boolean cleanupLocalDirOnStartup = true;
    private boolean createTransactionalTables = true;
    private boolean isMetastoreSecure;
    private String metastoreServerPrincipal;
    private String metastoreServerKeyTab;
    private int dataNodes = DEFAULT_DATANODE_COUNT;
    private int numInstances = DEFAULT_HS2_INSTANCES;

    public Builder() {
    }

    public Builder withMiniMR() {
      this.miniClusterType = MiniClusterType.MR;
      return this;
    }
    public Builder withMiniTez() {
      this.miniClusterType = MiniClusterType.TEZ;
      return this;
    }

    public Builder withMiniKdc(String serverPrincipal, String serverKeytab) {
      this.useMiniKdc = true;
      this.serverPrincipal = serverPrincipal;
      this.serverKeytab = serverKeytab;
      return this;
    }

    public Builder withAuthenticationType(String authType) {
      this.authType = authType;
      return this;
    }

    public Builder withTransactionalTables(boolean createTransactionalTables) {
      this.createTransactionalTables = createTransactionalTables;
      return this;
    }

    public Builder withRemoteMetastore() {
      this.isMetastoreRemote = true;
      return this;
    }

    public Builder withSecureRemoteMetastore(String metastoreServerPrincipal, String metastoreServerKeyTab) {
      this.isMetastoreRemote = true;
      this.isMetastoreSecure = true;
      this.metastoreServerPrincipal = metastoreServerPrincipal;
      this.metastoreServerKeyTab = metastoreServerKeyTab;
      return this;
    }

    public Builder withConf(HiveConf hiveConf) {
      this.hiveConf = hiveConf;
      return this;
    }

    public Builder withHA() {
      this.isHA = true;
      return this;
    }

    /**
     * Start HS2 with HTTP transport mode, default is binary mode
     * @return this Builder
     */
    public Builder withHTTPTransport(){
      this.isHTTPTransMode = true;
      return this;
    }

    public Builder cleanupLocalDirOnStartup(boolean val) {
      this.cleanupLocalDirOnStartup = val;
      return this;
    }

    /**
     * Set the number of datanodes to be used by HS2.
     * @param count the number of datanodes
     * @return this Builder
     */
    public Builder withDataNodes(int count) {
      this.dataNodes = count;
      return this;
    }

    /**
     * Set the number of HS2 instances in the cluster.
     * @param count the number of HS2 instances
     * @return this Builder
     */
    public Builder withNumInstances(int count) {
      this.numInstances = count;
      return this;
    }

    public MiniHS2JVMCluster build() throws Exception {
      if (miniClusterType == MiniClusterType.MR && useMiniKdc) {
        throw new IOException("Can't create secure miniMr ... yet");
      }
      Iterator<Map.Entry<String, String>> iter = hiveConf.iterator();
      while (iter.hasNext()) {
        String key = iter.next().getKey();
        hiveConf.set(key, hiveConf.get(key));
      }
      if (isHTTPTransMode) {
        hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, HS2_HTTP_MODE);
      } else {
        hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, HS2_BINARY_MODE);
      }
      return new MiniHS2JVMCluster(hiveConf, miniClusterType, useMiniKdc, serverPrincipal, serverKeytab,
          isMetastoreRemote, createTransactionalTables, usePortsFromConf, authType, isHA, cleanupLocalDirOnStartup,
          isMetastoreSecure, metastoreServerPrincipal, metastoreServerKeyTab, dataNodes, numInstances);
    }
  }

  public MiniMrShim getMr() {
    return mr;
  }

  public void setMr(MiniMrShim mr) {
    this.mr = mr;
  }

  public MiniDFSShim getDfs() {
    return dfs;
  }

  public void setDfs(MiniDFSShim dfs) {
    this.dfs = dfs;
  }

  public FileSystem getLocalFS() {
    return localFS;
  }

  public MiniClusterType getMiniClusterType() {
    return miniClusterType;
  }

  public void setMiniClusterType(MiniClusterType miniClusterType) {
    this.miniClusterType = miniClusterType;
  }

  public boolean isUseMiniKdc() {
    return useMiniKdc;
  }

  /**
   * Get the number of HS2 instances in the cluster
   * @return number of instances
   */
  public int getNumInstances() {
    return numInstances;
  }

  /**
   * Get a specific HS2 instance by index
   * @param index the instance index (0-based)
   * @return the MiniHS2 instance
   */
  public MiniHS2 getInstance(int index) {
    if (index < 0 || index >= hs2Instances.size()) {
      throw new IllegalArgumentException("Invalid instance index: " + index);
    }
    return hs2Instances.get(index);
  }

  /**
   * Get all HS2 instances in the cluster
   * @return list of all instances
   */
  public List<MiniHS2> getAllInstances() {
    return new ArrayList<>(hs2Instances);
  }

  /**
   * Get the next available HS2 instance in round-robin fashion
   * @return the next MiniHS2 instance
   */
  public MiniHS2 getNextInstance() {
    if (hs2Instances.isEmpty()) {
      throw new IllegalStateException("No HS2 instances available");
    }
    MiniHS2 instance = hs2Instances.get(currentInstanceIndex);
    currentInstanceIndex = (currentInstanceIndex + 1) % hs2Instances.size();
    return instance;
  }

  private MiniHS2JVMCluster(HiveConf hiveConf, MiniClusterType miniClusterType, boolean useMiniKdc,
      String serverPrincipal, String serverKeytab, boolean isMetastoreRemote, boolean createTransactionalTables,
      boolean usePortsFromConf, String authType, boolean isHA, boolean cleanupLocalDirOnStartup,
      boolean isMetastoreSecure, String metastoreServerPrincipal, String metastoreKeyTab,
      int dataNodes, int numInstances) throws Exception {
    
    // Initialize with the first instance's ports for compatibility
    super(
        hiveConf,
        "localhost",
        (usePortsFromConf ? hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT) : MetaStoreTestUtils
            .findFreePort()),
        (usePortsFromConf ? hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT) : MetaStoreTestUtils
            .findFreePort()),
        (usePortsFromConf ? hiveConf.getIntVar(ConfVars.HIVE_SERVER2_WEBUI_PORT) : MetaStoreTestUtils
            .findFreePort()));
    hiveConf.setLongVar(ConfVars.HIVE_SERVER2_MAX_START_ATTEMPTS, 3l);
    hiveConf.setTimeVar(ConfVars.HIVE_SERVER2_SLEEP_INTERVAL_BETWEEN_START_ATTEMPTS, 10,
        TimeUnit.SECONDS);
    hiveConf.setBoolVar(ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_ENABLED, false);
    this.miniClusterType = miniClusterType;
    this.useMiniKdc = useMiniKdc;
    this.serverPrincipal = serverPrincipal;
    this.isMetastoreRemote = isMetastoreRemote;
    this.isMetastoreSecure = isMetastoreSecure;
    this.cleanupLocalDirOnStartup = cleanupLocalDirOnStartup;
    this.usePortsFromConf = usePortsFromConf;
    this.createTransactionalTables = createTransactionalTables;
    this.numInstances = numInstances;
    baseDir = getBaseDir();
    localFS = FileSystem.getLocal(hiveConf);
    FileSystem fs;

    if (miniClusterType != MiniClusterType.LOCALFS_ONLY) {
      // Initialize dfs
      dfs = ShimLoader.getHadoopShims().getMiniDfs(hiveConf, dataNodes, true, null, isHA);
      fs = dfs.getFileSystem();
      String uriString = fs.getUri().toString();

      // Initialize the execution engine based on cluster type
      switch (miniClusterType) {
      case TEZ:
        // Change the engine to tez
        hiveConf.setVar(ConfVars.HIVE_EXECUTION_ENGINE, "tez");
        mr = ShimLoader.getHadoopShims().getMiniTezCluster(hiveConf, 2, uriString, false);
        break;
      case LLAP:
        if (usePortsFromConf) {
          hiveConf.setBoolean("minillap.usePortsFromConf", true);
        }
        llapCluster = LlapItUtils.startAndGetMiniLlapCluster(hiveConf, null, null);

        mr = ShimLoader.getHadoopShims().getMiniTezCluster(hiveConf, 2, uriString, true);
        break;
      case MR:
        mr = ShimLoader.getHadoopShims().getMiniMrCluster(hiveConf, 2, uriString, 1);
        break;
      default:
        throw new IllegalArgumentException("Unsupported cluster type " + mr);
      }
      // store the config in system properties
      mr.setupConfiguration(getHiveConf());
      baseFsDir = new Path(new Path(fs.getUri()), "/base");
    } else {
      // This is FS only mode, just initialize the dfs root directory.
      fs = FileSystem.getLocal(hiveConf);
      baseFsDir = new Path("file://" + baseDir.toURI().getPath());

      if (cleanupLocalDirOnStartup) {
        // Cleanup baseFsDir since it can be shared across tests.
        LOG.info("Attempting to cleanup baseFsDir: {} while setting up MiniHS2JVMCluster", baseDir);
        Preconditions.checkState(baseFsDir.depth() >= 3); // Avoid "/tmp", directories closer to "/"
        fs.delete(baseFsDir, true);
      }
    }
    if (useMiniKdc) {
      hiveConf.setVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL, serverPrincipal);
      hiveConf.setVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB, serverKeytab);
      hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, authType);
    }

    if (isMetastoreSecure) {
      hiveConf.setVar(ConfVars.METASTORE_KERBEROS_PRINCIPAL, metastoreServerPrincipal);
      hiveConf.setVar(ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, metastoreKeyTab);
      hiveConf.setBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL, true);
    }

    fs.mkdirs(baseFsDir);
    Path wareHouseDir = new Path(baseFsDir, "warehouse");
    // Create warehouse with 777, so that user impersonation has no issues.
    FileSystem.mkdirs(fs, wareHouseDir, FULL_PERM);
    
    fs.mkdirs(wareHouseDir);
    setWareHouseDir(wareHouseDir.toString());
    if (!usePortsFromConf) {
      // reassign a new port, just in case if one of the MR services grabbed the last one
      setBinaryPort(MetaStoreTestUtils.findFreePort());
    }
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, getHost());
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, getBinaryPort());
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT, getHttpPort());
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_WEBUI_PORT, getWebPort());

    Path scratchDir = new Path(baseFsDir, "scratch");
    // Create root scratchdir with write all, so that user impersonation has no issues.
    Utilities.createDirsWithPermission(hiveConf, scratchDir, WRITE_ALL_PERM, true);
    System.setProperty(HiveConf.ConfVars.SCRATCH_DIR.varname, scratchDir.toString());
    hiveConf.setVar(ConfVars.SCRATCH_DIR, scratchDir.toString());

    String localScratchDir = baseDir.getPath() + File.separator + "scratch";
    System.setProperty(HiveConf.ConfVars.LOCAL_SCRATCH_DIR.varname, localScratchDir);
    hiveConf.setVar(ConfVars.LOCAL_SCRATCH_DIR, localScratchDir);

    // Create multiple HS2 instances with shared resources
    createHS2Instances(hiveConf, fs, wareHouseDir, scratchDir, serverKeytab, metastoreKeyTab, metastoreServerPrincipal);
  }

  private void createHS2Instances(HiveConf baseHiveConf, FileSystem fs, Path wareHouseDir, Path scratchDir, String serverKeytab, String metastoreKeyTab, String metastoreServerPrincipal) 
      throws Exception {
    
    for (int i = 0; i < numInstances; i++) {
      // Create a copy of the base configuration for this instance
      HiveConf instanceConf = new HiveConf(baseHiveConf);
      
      // Assign unique ports for this instance
      int binaryPort = usePortsFromConf ? 
          baseHiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT) : 
          MetaStoreTestUtils.findFreePort();
      int httpPort = usePortsFromConf ? 
          baseHiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT) : 
          MetaStoreTestUtils.findFreePort();
      int webPort = usePortsFromConf ? 
          baseHiveConf.getIntVar(ConfVars.HIVE_SERVER2_WEBUI_PORT) : 
          MetaStoreTestUtils.findFreePort();

      // Configure this instance
      instanceConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, getHost());
      instanceConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, binaryPort);
      instanceConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT, httpPort);
      instanceConf.setIntVar(ConfVars.HIVE_SERVER2_WEBUI_PORT, webPort);

      // Set up Kerberos if enabled
      if (useMiniKdc) {
        instanceConf.setVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL, serverPrincipal);
        instanceConf.setVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB, serverKeytab);
        instanceConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "KERBEROS");
      }

      if (isMetastoreSecure) {
        instanceConf.setVar(ConfVars.METASTORE_KERBEROS_PRINCIPAL, metastoreServerPrincipal);
        instanceConf.setVar(ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, metastoreKeyTab);
        instanceConf.setBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL, true);
      }

      // Create MiniHS2 instance with shared resources
      MiniHS2 instance = new MiniHS2(instanceConf, MiniHS2.MiniClusterType.valueOf(miniClusterType.name()), usePortsFromConf, isMetastoreRemote);

      // Share the resources with this instance
      instance.setMr(mr);
      instance.setDfs(dfs);
      instance.setWareHouseDir(wareHouseDir.toString());

      hs2Instances.add(instance);
    }
  }

  // Compatibility constructors
  public MiniHS2JVMCluster(HiveConf hiveConf) throws Exception {
    this(hiveConf, MiniClusterType.LOCALFS_ONLY);
  }

  public MiniHS2JVMCluster(HiveConf hiveConf, MiniClusterType clusterType) throws Exception {
    this(hiveConf, clusterType, false, false);
  }

  public MiniHS2JVMCluster(HiveConf hiveConf, MiniClusterType clusterType, boolean usePortsFromConf, boolean isMetastoreRemote)
      throws Exception {
    this(hiveConf, clusterType, false, null, null,
        isMetastoreRemote, true, usePortsFromConf, "KERBEROS", false, true,
        false, null, null, DEFAULT_DATANODE_COUNT, DEFAULT_HS2_INSTANCES);
  }

  public void start(Map<String, String> confOverlay) throws Exception {
    if (isMetastoreRemote) {
      hmsPort = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), getHiveConf(),
              false, false, false, false, createTransactionalTables);
      setWareHouseDir(MetastoreConf.getVar(getHiveConf(), MetastoreConf.ConfVars.WAREHOUSE));
    }

    // Set confOverlay parameters
    for (Map.Entry<String, String> entry : confOverlay.entrySet()) {
      setConfProperty(entry.getKey(), entry.getValue());
    }

    // Start all HS2 instances with retry logic similar to MiniHS2
    for (MiniHS2 instance : hs2Instances) {
      Exception hs2Exception = null;
      boolean hs2Started = false;
      
      // Retry logic similar to MiniHS2
      for (int tryCount = 0; tryCount < MetaStoreTestUtils.RETRY_COUNT; tryCount++) {
        try {
          instance.start(confOverlay);
          hs2Started = true;
          break;
        } catch (Exception t) {
          hs2Exception = t;
          if (usePortsFromConf) {
            hs2Started = false;
            break;
          } else {
            // For cluster mode, we don't reassign ports since each instance has its own ports
            // Just continue to next retry
            LOG.warn("Retry {} failed for HS2 instance, will retry", tryCount + 1);
          }
        }
      }
      
      if (!hs2Started) {
        LOG.error("Failed to start HS2 instance after retries", hs2Exception);
        // Continue with other instances even if one fails
      }
    }

    // Wait for startup for all instances
    for (MiniHS2 instance : hs2Instances) {
      try {
        waitForStartup(instance);
      } catch (Exception e) {
        LOG.warn("Error waiting for HS2 instance startup", e);
      }
    }

    setStarted(true);
  }

  private void resetSamlACSUrl() throws URISyntaxException {
    if (isSAMLAuth()) {
      // in case this is a SAML Auth miniHS2 we should make sure that the
      // assertion consumer service url is appropriately reconfigured if the http
      // port changed.
      String existingAcs = HiveConf
          .getVar(getHiveConf(), ConfVars.HIVE_SERVER2_SAML_CALLBACK_URL);
      String existingPort = String.valueOf(new URI(existingAcs).getPort());
      String newAcs = existingAcs.replace(":" + existingPort, ":" + HiveConf
          .getVar(getHiveConf(), ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT));
      HiveConf.setVar(getHiveConf(), ConfVars.HIVE_SERVER2_SAML_CALLBACK_URL, newAcs);
    }
  }

  private boolean isSAMLAuth() {
    return "SAML"
        .equals(HiveConf.getVar(getHiveConf(), ConfVars.HIVE_SERVER2_SAML_CALLBACK_URL));
  }  
  
  public void graceful_stop() {
    verifyStarted();
    for (MiniHS2 instance : hs2Instances) {
      try {
        instance.graceful_stop();
      } catch (Exception e) {
        LOG.warn("Error during graceful stop of HS2 instance", e);
      }
    }
  }

  public void stop() {
    verifyStarted();
    
    // Stop all HS2 instances
    for (MiniHS2 instance : hs2Instances) {
      try {
        instance.stop();
      } catch (Exception e) {
        LOG.warn("Error stopping HS2 instance", e);
      }
    }
    
    setStarted(false);
    try {
      if (llapCluster != null) {
        llapCluster.stop();
      }
      if (mr != null) {
        mr.shutdown();
        mr = null;
      }
      if (dfs != null) {
        dfs.shutdown();
        dfs = null;
      }
    } catch (IOException e) {
      // Ignore errors cleaning up miniMR
    }
  }

  public void cleanup() {
    for (MiniHS2 instance : hs2Instances) {
      try {
        instance.cleanup();
      } catch (Exception e) {
        LOG.warn("Error cleaning up HS2 instance", e);
      }
    }
    FileUtils.deleteQuietly(baseDir);
  }

  public boolean isLeader() {
    // Return true if any instance is leader
    for (MiniHS2 instance : hs2Instances) {
      if (instance.isLeader()) {
        return true;
      }
    }
    return false;
  }

  public SettableFuture<Boolean> getIsLeaderTestFuture() {
    // Return the first instance's future for compatibility
    return hs2Instances.isEmpty() ? null : hs2Instances.get(0).getIsLeaderTestFuture();
  }

  public SettableFuture<Boolean> getNotLeaderTestFuture() {
    // Return the first instance's future for compatibility
    return hs2Instances.isEmpty() ? null : hs2Instances.get(0).getNotLeaderTestFuture();
  }

  public void setPamAuthenticator(final PamAuthenticator pamAuthenticator) {
    for (MiniHS2 instance : hs2Instances) {
      instance.setPamAuthenticator(pamAuthenticator);
    }
  }

  public int getOpenSessionsCount() {
    int totalSessions = 0;
    for (MiniHS2 instance : hs2Instances) {
      totalSessions += instance.getOpenSessionsCount();
    }
    return totalSessions;
  }

  public CLIServiceClient getServiceClient() {
    verifyStarted();
    return getNextInstance().getServiceClient();
  }

  public HiveConf getServerConf() {
    return hs2Instances.isEmpty() ? null : hs2Instances.get(0).getServerConf();
  }

  public CLIServiceClient getServiceClientInternal() {
    return getNextInstance().getServiceClientInternal();
  }

  /**
   * Get JDBC URL for the next available instance (round-robin)
   * @return JDBC URL
   * @throws Exception
   */
  public String getJdbcURL() throws Exception {
    return getNextInstance().getJdbcURL();
  }

  /**
   * Get JDBC URL for the next available instance (round-robin)
   * @param dbName - DB name to be included in the URL
   * @return 
   * @throws Exception
   */
  public String getJdbcURL(String dbName) throws Exception {
    return getNextInstance().getJdbcURL(dbName);
  }

  /**
   * Get JDBC URL for the next available instance (round-robin)
   * @param dbName - DB name to be included in the URL
   * @param sessionConfExt - Additional string to be appended to sessionConf part of url
   * @return 
   * @throws Exception
   */
  public String getJdbcURL(String dbName, String sessionConfExt) throws Exception {
    return getNextInstance().getJdbcURL(dbName, sessionConfExt);
  }

  /**
   * Get JDBC URL for the next available instance (round-robin)
   * @param dbName - DB name to be included in the URL
   * @param sessionConfExt - Additional string to be appended to sessionConf part of url
   * @param hiveConfExt - Additional string to be appended to HiveConf part of url (excluding the ?)
   * @return JDBC URL
   * @throws Exception
   */
  public String getJdbcURL(String dbName, String sessionConfExt, String hiveConfExt) throws Exception {
    return getNextInstance().getJdbcURL(dbName, sessionConfExt, hiveConfExt);
  }

  /**
   * Build base JDBC URL for the next available instance
   * @return base JDBC URL
   */
  public String getBaseJdbcURL() {
    return getNextInstance().getBaseJdbcURL();
  }

  /**
   * Build base HTTP JDBC URL for the next available instance
   * @return base HTTP JDBC URL
   */
  public String getBaseHttpJdbcURL() {
    return getNextInstance().getBaseHttpJdbcURL();
  }
  private String getZKBaseJdbcURL() throws Exception {
    HiveConf hiveConf = getServerConf();
    if (hiveConf != null) {
      String zkEnsemble =  hiveConf.getZKConfig().getQuorumServers();
      return "jdbc:hive2://" + zkEnsemble + "/";
    }
    throw new Exception("Server's HiveConf is null. Unable to read ZooKeeper configs.");
  }  

  /**
   * Returns HTTP connection URL for the next available instance
   * @return URL
   * @throws Exception
   */
  public synchronized String getHttpJdbcURL() throws Exception {
    return getNextInstance().getHttpJdbcURL();
  }





  private boolean isHttpTransportMode() {
    String transportMode = getConfProperty(ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname);
    return transportMode != null && (transportMode.equalsIgnoreCase(HS2_HTTP_MODE));
  }

  private boolean isDynamicServiceDiscovery() throws Exception {
    HiveConf hiveConf = getServerConf();
    if (hiveConf == null) {
      throw new Exception("Server's HiveConf is null. Unable to read ZooKeeper configs.");
    }
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY)) {
      return true;
    }
    return false;
  }

  public static String getJdbcDriverName() {
    return driverName;
  }

  public MiniMrShim getMR() {
    return mr;
  }

  public MiniDFSShim getDFS() {
    return dfs;
  }  

  private void waitForStartup(MiniHS2 instance) throws Exception {
    int waitTime = 0;
    long startupTimeout = 1000L * 1000L;
    CLIServiceClient hs2Client = instance.getServiceClientInternal();
    SessionHandle sessionHandle = null;
    do {
      Thread.sleep(500L);
      waitTime += 500L;
      if (waitTime > startupTimeout) {
        throw new TimeoutException("Couldn't access new HiveServer2: " + instance.getJdbcURL());
      }
      try {
        Map <String, String> sessionConf = new HashMap<String, String>();
        /**
        if (isUseMiniKdc()) {
          getMiniKdc().loginUser(getMiniKdc().getDefaultUserPrincipal());
          sessionConf.put("principal", serverPrincipal);
        }
         */
        sessionHandle = hs2Client.openSession("foo", "bar", sessionConf);
      } catch (Exception e) {
        if (e.getMessage().contains("Cannot open sessions on an inactive HS2")) {
          // Passive HS2 has started. TODO: seems fragile
          return;
        }
        // service not started yet
        continue;
      }
      hs2Client.closeSession(sessionHandle);
      break;
    } while (true);
  }

  public Service.STATE getState() {
    // Return the state of the first instance for compatibility
    return hs2Instances.isEmpty() ? Service.STATE.NOTINITED : hs2Instances.get(0).getState();
  }

  static File getBaseDir() {
    File baseDir = new File(tmpDir + "/local_base");
    return baseDir;
  }

  public static void cleanupLocalDir() throws IOException {
    File baseDir = getBaseDir();
    try {
      org.apache.hadoop.hive.common.FileUtils.deleteDirectory(baseDir);
    } catch (FileNotFoundException e) {
      // Ignore. Safe if it does not exist.
    }
  }

  public int getHmsPort() {
    return hmsPort;
  }  
}
