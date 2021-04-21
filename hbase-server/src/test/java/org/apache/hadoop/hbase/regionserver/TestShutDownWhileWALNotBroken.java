package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.wal.AsyncFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.AsyncProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.FSHLogProvider;
import org.apache.hadoop.hbase.wal.NettyAsyncFSWALConfigHelper;
import org.apache.hadoop.hbase.wal.TestAsyncFSWALCorruptionDueToDanglingByteBuffer;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hbase.regionserver.TestHRegion.TEST_UTIL;
import static org.apache.hadoop.hbase.util.Threads.sleep;
import static org.junit.Assert.assertTrue;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestShutDownWhileWALNotBroken {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestShutDownWhileWALNotBroken.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestShutDownWhileWALNotBroken.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("TestShutDownWhileWALNotBroken");

  private static byte[] CF = Bytes.toBytes("CF");

  public String walType = "multiwal";

  public static final class MyRegionServer extends HRegionServer {

    private final CountDownLatch latch = new CountDownLatch(1);

    public MyRegionServer(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    public void abort(String reason, Throwable cause) {
      if (cause instanceof KeeperException.SessionExpiredException) {
        // called from ZKWatcher, let's wait a bit to make sure that we call stop before calling
        // abort.
        try {
          latch.await();
        } catch (InterruptedException e) {
        }
      } else {
        // abort from other classes, usually LogRoller, now we can make progress on abort.
        latch.countDown();
      }
      super.abort(reason, cause);
    }
  }

  public static final class MyWALWriter extends AsyncProtobufLogWriter {

    public MyWALWriter(EventLoopGroup eventLoopGroup, Class<? extends Channel> channelClass) {
      super(eventLoopGroup, channelClass);
    }

    @Override
    public CompletableFuture<Long> sync(boolean forceSync) {
      CompletableFuture<Long> future = new CompletableFuture<>();
      sleep(10000);
      future.completeExceptionally(new IOException("stream already broken"));
      return future;
    }
  }

  @Before
  public void setUp() throws Exception {
    UTIL.getConfiguration().setClass(HConstants.REGION_SERVER_IMPL, MyRegionServer.class,
      HRegionServer.class);
    UTIL.getConfiguration().setClass("hbase.regionserver.hlog.writer.impl",
      MyWALWriter.class, WALProvider.AsyncWriter.class);
    UTIL.getConfiguration().set(WALFactory.WAL_PROVIDER, walType);
    UTIL.startMiniCluster(2);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    int colCount = 10;
    TableDescriptor htd =
      TableDescriptorBuilder.newBuilder(TableName.valueOf("aaaaaaa"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("column")).build();
    NavigableMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : htd.getColumnFamilyNames()) {
      scopes.put(fam, 0);
    }
    byte[] row = Bytes.toBytes("row");
    final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl(1);

    // Write columns named 1, 2, 3, etc. and then values of single byte
    // 1, 2, 3...
    long timestamp = System.currentTimeMillis();
    WALEdit cols = new WALEdit();
    for (int i = 0; i < colCount; i++) {
      cols.add(new KeyValue(row, Bytes.toBytes("column"),
        Bytes.toBytes(Integer.toString(i)),
        timestamp, new byte[] { (byte)(i + '0') }));
    }
    RegionInfo hri = RegionInfoBuilder.newBuilder(htd.getTableName()).build();

    ServerName currentServername = ServerName.valueOf("aaaaa", 16010, 1);
    WALFactory wals = new WALFactory(UTIL.getConfiguration(), currentServername.toString());
    final AsyncFSWAL log = (AsyncFSWAL)wals.getWAL(hri);
    final long txid = log.appendData(hri, new WALKeyImpl(hri.getEncodedNameAsBytes(),
      htd.getTableName(), System.currentTimeMillis(), mvcc, scopes), cols);
    log.sync();
    log.rollWriter(true);
    timestamp = System.currentTimeMillis();
    cols = new WALEdit();
    for (int i = 0; i < colCount; i++) {
      cols.add(new KeyValue(row, Bytes.toBytes("column"),
        Bytes.toBytes(Integer.toString(i)),
        timestamp, new byte[] { (byte)(i + '0') }));
    }
    log.appendData(hri, new WALKeyImpl(hri.getEncodedNameAsBytes(),
      htd.getTableName(), System.currentTimeMillis(), mvcc, scopes), cols);
    //let synwriter timeout
    log.sync();
    log.shutdown();
  }


//  public static final class PauseWAL extends AsyncFSWAL {
//
//    public PauseWAL(FileSystem fs, Path rootDir, String logDir, String archiveDir,
//      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
//      String prefix, String suffix, EventLoopGroup eventLoopGroup,
//      Class<? extends Channel> channelClass) throws FailedLogCloseException, IOException {
//      super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix,
//        eventLoopGroup, channelClass);
//    }
//  }
//
//  public static final class PauseWALProvider extends
//    AbstractFSWALProvider<TestAsyncFSWALCorruptionDueToDanglingByteBuffer.PauseWAL> {
//
//    private EventLoopGroup eventLoopGroup;
//
//    private Class<? extends Channel> channelClass;
//
//    @Override
//    protected TestAsyncFSWALCorruptionDueToDanglingByteBuffer.PauseWAL createWAL() throws IOException {
//      return new TestAsyncFSWALCorruptionDueToDanglingByteBuffer.PauseWAL(CommonFSUtils.getWALFileSystem(conf), CommonFSUtils.getWALRootDir(conf),
//        getWALDirectoryName(factory.factoryId), getWALArchiveDirectoryName(conf, factory.factoryId),
//        conf, listeners, true, logPrefix,
//        META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null, eventLoopGroup,
//        channelClass);
//    }
//
//    @Override
//    protected void doInit(Configuration conf) throws IOException {
//      Pair<EventLoopGroup, Class<? extends Channel>> eventLoopGroupAndChannelClass =
//        NettyAsyncFSWALConfigHelper.getEventLoopConfig(conf);
//      eventLoopGroup = eventLoopGroupAndChannelClass.getFirst();
//      channelClass = eventLoopGroupAndChannelClass.getSecond();
//    }
//  }
}
