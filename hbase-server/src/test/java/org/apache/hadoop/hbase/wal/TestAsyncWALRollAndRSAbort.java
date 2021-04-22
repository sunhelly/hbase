package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TestClientOperationTimeout;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.wal.AsyncFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hbase.util.Threads.sleep;

public class TestAsyncWALRollAndRSAbort {
  private static final Logger LOG =
    LoggerFactory.getLogger(TestAsyncWALRollAndRSAbort.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private final static int SYNC_TIMEOUT_MILLIS = 10;

  static class FailAsyncFSWAL extends AsyncFSWAL {
    private boolean shutdownComplete = false;
    private int syncCount = 0;
    private AtomicBoolean shouldSyncFail = new AtomicBoolean(false);

    public FailAsyncFSWAL(FileSystem fs, Path rootDir, String logDir, String archiveDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
      String prefix, String suffix, EventLoopGroup eventLoopGroup,
      Class<? extends Channel> channelClass) throws FailedLogCloseException, IOException {
      super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix,
        eventLoopGroup, channelClass);
    }

    @Override
    protected WALProvider.AsyncWriter createWriterInstance(Path path) throws IOException {
      WALProvider.AsyncWriter writer = super.createWriterInstance(path);
      return new WALProvider.AsyncWriter() {

        @Override
        public void close() throws IOException {
          writer.close();
        }

        @Override
        public long getLength() {
          return writer.getLength();
        }

        @Override
        public long getSyncedLength() {
          return writer.getSyncedLength();
        }

        @Override
        public CompletableFuture<Long> sync(boolean forceSync) {
          LOG.info("writer sync, should not fail...forceSync=" + forceSync);
          CompletableFuture<Long> completableFuture = new CompletableFuture<>();
          if (shouldSyncFail.get()) {
            LOG.info("writer sync, should fail...");
            sleep(SYNC_TIMEOUT_MILLIS);
            completableFuture.completeExceptionally(new IOException("sync timeout"));
            return completableFuture;
          }
          LOG.info("writer sync, should not fail...");
          return writer.sync(forceSync);
        }

        @Override
        public void append(Entry entry) {
          writer.append(entry);
        }
      };
    }

    @Override
    public void sync(boolean forceSync) throws IOException {
      super.sync(forceSync);
    }

    @Override
    public void sync(long txid, boolean forceSync) throws IOException {
      super.sync(txid, forceSync);
    }

    @Override
    public void doShutdown() throws IOException{
      super.doShutdown();
      shutdownComplete = true;
    }

    public boolean doesShutDownComplete() {
      return shutdownComplete;
    }

    public void setShouldSyncFail(boolean shouldSyncFail){
      this.shouldSyncFail.set(shouldSyncFail);
    }
  }
  
  public static final class FailAsyncFSWALProvider extends AbstractFSWALProvider<FailAsyncFSWAL> {

    private EventLoopGroup eventLoopGroup;

    private Class<? extends Channel> channelClass;

    @Override
    protected FailAsyncFSWAL createWAL() throws IOException {
      return new FailAsyncFSWAL(CommonFSUtils.getWALFileSystem(conf), CommonFSUtils.getWALRootDir(conf),
        getWALDirectoryName(factory.factoryId), getWALArchiveDirectoryName(conf, factory.factoryId),
        conf, listeners, true, logPrefix,
        META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null, eventLoopGroup,
        channelClass);
    }

    @Override
    protected void doInit(Configuration conf) throws IOException {
      Pair<EventLoopGroup, Class<? extends Channel>> eventLoopGroupAndChannelClass =
        NettyAsyncFSWALConfigHelper.getEventLoopConfig(conf);
      eventLoopGroup = eventLoopGroupAndChannelClass.getFirst();
      channelClass = eventLoopGroupAndChannelClass.getSecond();
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setClass(WALFactory.WAL_PROVIDER, FailAsyncFSWALProvider.class,
      WALProvider.class);
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRSAbortWithUnflushedEdits() throws Exception {
    // Create the test table and open it
    TableName tableName = TableName.valueOf(this.getClass().getSimpleName());
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();

    UTIL.getAdmin().createTable(desc);
    Table table = UTIL.getConnection().getTable(tableName);
    try {
      HRegionServer server = UTIL.getRSForFirstRegionInTable(tableName);
      FailAsyncFSWAL log = (FailAsyncFSWAL)server.getWAL(null);

      Put p = new Put(Bytes.toBytes("row2001"));
      p.addColumn(HConstants.CATALOG_FAMILY, Bytes.toBytes("col"), Bytes.toBytes(2001));
      table.put(p);

      p = new Put(Bytes.toBytes("row2002"));
      p.addColumn(HConstants.CATALOG_FAMILY, Bytes.toBytes("col"), Bytes.toBytes(2002));
      table.put(p);
      LOG.info("======================sync================");
      log.sync();
      LOG.info("======================roll writer================");
      log.rollWriter(true);
      sleep(10000);

      table.put(new Put(Bytes.toBytes("row")).addColumn(HConstants.CATALOG_FAMILY,
        Bytes.toBytes("col"), Bytes.toBytes("value")));

      LOG.info("======================sync fail================");
      log.setShouldSyncFail(true);
      log.sync(true);

      LOG.info("======================log shutdown================");
      log.shutdown();
      UTIL.waitFor(SYNC_TIMEOUT_MILLIS * 2, log::doesShutDownComplete);
//      UTIL.getMiniHBaseCluster().abortRegionServer(0);
    } finally {
      table.close();
    }
  }

}
