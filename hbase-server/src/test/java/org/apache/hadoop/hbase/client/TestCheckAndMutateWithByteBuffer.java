package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.io.ByteBuffAllocator.BYTEBUFF_ALLOCATOR_CLASS;
import static org.apache.hadoop.hbase.io.ByteBuffAllocator.MIN_ALLOCATE_SIZE_KEY;
import static org.apache.hadoop.hbase.io.hfile.BlockCacheFactory.BUCKET_CACHE_WRITER_THREADS_KEY;
import static org.apache.hadoop.hbase.util.Threads.sleep;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.StripeStoreEngine;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestCheckAndMutateWithByteBuffer {
  private static final Logger LOG = LoggerFactory.getLogger(TestCheckAndMutateWithByteBuffer.class);
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCheckAndMutateWithByteBuffer.class);
  @Rule
  public TestName name = new TestName();

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private static Admin admin = null;

  private static final byte[] COLUMN = Bytes.toBytes("A");
  private static final byte[] CHECK_ROW = Bytes.toBytes("checkRow");
  private static final byte[] CHECK_QUALIFIER = Bytes.toBytes("checkq");
  private static final byte[] CHECK_VALUE = Bytes.toBytes("checkValue");

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf.setStrings(HConstants.REGION_IMPL, CheckAndMutateBlockRegion.class.getName());
    conf.setBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    conf.setInt("hbase.client.retries.number", 1);
    conf.setInt(MIN_ALLOCATE_SIZE_KEY, 1);
    conf.setInt(BUCKET_CACHE_WRITER_THREADS_KEY, 20);
    conf.setInt(ByteBuffAllocator.BUFFER_SIZE_KEY, 1024);
    conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    conf.setInt(HConstants.BUCKET_CACHE_SIZE_KEY, 64);
    conf.setInt(MIN_ALLOCATE_SIZE_KEY, 1);
//    conf.getBoolean(BUCKET_CACHE_COMPOSITE_KEY, true);
    conf.set(BYTEBUFF_ALLOCATOR_CLASS, TestDeallocateRewriteByteBuffAllocator.class.getName());
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, StripeStoreEngine.class.getName());
    TEST_UTIL.startMiniCluster();
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCompactionAndCheckAndPut() throws Exception {
    Table testTable = createTable(TEST_UTIL, TableName.valueOf("t1"));
    Put put = new Put(CHECK_ROW);
    put.addColumn(COLUMN, CHECK_QUALIFIER, CHECK_VALUE);
    testTable.put(put);
    admin.flush(testTable.getName());

    assertTrue(
      testTable.checkAndMutate(CHECK_ROW, COLUMN).qualifier(CHECK_QUALIFIER).ifEquals(CHECK_VALUE)
        .thenPut(new Put(CHECK_ROW).addColumn(COLUMN, Bytes.toBytes("cq"),
          Bytes.toBytes("testValue"))));
  }

  private static Table createTable(HBaseTestingUtility util, TableName tableName)
    throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN).setBlocksize(100).build())
      .build();
    return util.createTable(td, null);
  }

  /**
   * An override of HRegion that allows us park compactions in a holding pattern and
   * then when appropriate for the test, allow them proceed again.
   */
  public static class CheckAndMutateBlockRegion extends HRegion {
    public CheckAndMutateBlockRegion(Path tableDir, WAL log, FileSystem fs, Configuration confParam,
      RegionInfo info, TableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, log, fs, confParam, info, htd, rsServices);
    }

    public CheckAndMutateBlockRegion(HRegionFileSystem fs, WAL wal, Configuration confParam,
      TableDescriptor htd, RegionServerServices rsServices) {
      super(fs, wal, confParam, htd, rsServices);
    }

    @Override
    public List<Cell> get(Get get, boolean withCoprocessor) throws IOException {
      List<Cell> cells = super.get(get, withCoprocessor);
      sleep(1000);
      LOG.info("get complete");
      return cells;
    }
  }

  private static class TestDeallocateRewriteByteBuffAllocator
    extends ByteBuffAllocator {
    TestDeallocateRewriteByteBuffAllocator(boolean reservoirEnabled, int maxBufCount, int bufSize,
      int minSizeForReservoirUse) {
      super(reservoirEnabled, maxBufCount, bufSize, minSizeForReservoirUse);
    }

    @Override
    protected void putbackBuffer(ByteBuffer buf) {
      if (buf.capacity() != bufSize || (reservoirEnabled ^ buf.isDirect())) {
        LOG.warn("Trying to put a buffer, not created by this pool! Will be just ignored");
        return;
      }
      buf.clear();
      byte[] tmp = generateTmpBytes(buf.capacity());
      buf.put(tmp, 0, tmp.length);

      super.putbackBuffer(buf);
    }

    private static byte[] generateTmpBytes(int length) {
      StringBuilder result = new StringBuilder();
      while (result.length() < length) {
        result.append("-");
      }
      return Bytes.toBytes(result.toString().substring(0, length));
    }
  }

}
