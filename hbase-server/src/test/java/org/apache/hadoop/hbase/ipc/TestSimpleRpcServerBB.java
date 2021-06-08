package org.apache.hadoop.hbase.ipc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({ LargeTests.class})
public class TestSimpleRpcServerBB {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSimpleRpcServerBB.class);

  @Rule
  public TestName name = new TestName();
  private static HBaseTestingUtility TEST_UTIL;

  private static TableName TABLE;
  private static byte[] FAMILY = Bytes.toBytes("f1");
  private static byte[] PRIVATE_COL = Bytes.toBytes("private");
  private static byte[] PUBLIC_COL = Bytes.toBytes("public");

  @Before
  public void setup() {
    TABLE = TableName.valueOf(name.getMethodName());
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().set(
      RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY,
      SimpleRpcServer.class.getName());
    TEST_UTIL.getConfiguration().setBoolean("hbase.server.allocator.pool.enabled", true);
    TEST_UTIL.getConfiguration().setInt("hbase.server.allocator.max.buffer.count", 1);
    TEST_UTIL.getConfiguration().setInt("hbase.server.allocator.buffer.size", 1024);

    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNettyRpcServer() throws Exception {
    final Table table = TEST_UTIL.createTable(TABLE, FAMILY);
    // Make a task in some other thread and leak it
    ExecutorService service = Executors.newFixedThreadPool(1);
    Future<Void> future = service.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        while(true) {
          MonitoredRPCHandler status = RpcServer.MONITORED_RPC.get();
          if(status != null) {
            status.toJSON();
            System.out.println("hahha");
          }
        }
      }
    });

    try {
      // put some test data
      List<Put> puts = new ArrayList<Put>(100);
      for (int i = 0; i < 100; i++) {
        Put p = new Put(Bytes.toBytes(i));
        p.addColumn(FAMILY, PRIVATE_COL, Bytes.toBytes("secret " + i));
        p.addColumn(FAMILY, PUBLIC_COL, Bytes.toBytes("info " + i));
        puts.add(p);
        table.put(p);
      }
      future.get();


      // read to verify it.
      Scan scan = new Scan();
      scan.setCaching(16);
      ResultScanner rs = table.getScanner(scan);
      int rowcnt = 0;
      for (Result r : rs) {
        rowcnt++;
        int rownum = Bytes.toInt(r.getRow());
        assertTrue(r.containsColumn(FAMILY, PRIVATE_COL));
        assertEquals("secret " + rownum,
          Bytes.toString(r.getValue(FAMILY, PRIVATE_COL)));
        assertTrue(r.containsColumn(FAMILY, PUBLIC_COL));
        assertEquals("info " + rownum,
          Bytes.toString(r.getValue(FAMILY, PUBLIC_COL)));
      }
      assertEquals("Expected 100 rows returned", 100, rowcnt);
    } finally {
      table.close();
    }
  }

}
