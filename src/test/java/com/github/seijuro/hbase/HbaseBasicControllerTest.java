package com.github.seijuro.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class HbaseBasicControllerTest {
    static class Sample {
        private static final String Tablename;

        private static final String ColumnFamily1;
        private static final String ColumnFamily2;

        private static final String Column1_1;
        private static final String Column1_2;
        private static final String Column1_3;

        private static final String Column2_1;
        private static final String Column2_2;
        private static final String Column2_3;

        private static final String ZKQuorum;
        private static final String ZKParent;

        private static final String RowKey;

        private static final String ColumnValue1_1;
        private static final String ColumnValue1_2;
        private static final String ColumnValue1_3;

        private static final int ColumnValue2_1;
        private static final int ColumnValue2_2;
        private static final int ColumnValue2_3;

        static {
            ZKQuorum = "kdfsname01,kdfsname02,kdfs01,kdfs02,kdfs03";
            ZKParent = "/hbase";

            Tablename = "test";

            ColumnFamily1 = "cf1";
            ColumnFamily2 = "cf2";

            Column1_1 = "column1";
            Column1_2 = "column2";
            Column1_3 = "column3";

            Column2_1 = "column4";
            Column2_2 = "column5";
            Column2_3 = "column6";


            RowKey = "TEST:CTRL:0001";

            ColumnValue1_1 = "column.11.value";
            ColumnValue1_2 = "column.12.value";
            ColumnValue1_3 = "column.13.value";

            ColumnValue2_1 = 21;
            ColumnValue2_2 = 22;
            ColumnValue2_3 = 23;
        }
    }

    @Test
    public void testInstantiation() {
        HBaseConfig config = new HBaseConfig.Builder()
                .setQuorum("kdfsname01,kdfsname02,kdfs01,kdfs02")
                .setDistributedCluster(true)
                .setZNodeParent("/hbase")
                .build();

        Configuration hbaseConfig = config.create();

        try {
            HBaseBasicController controller = new HBaseBasicController(hbaseConfig);

            Assert.assertNotNull(controller);

            return;
        }
        catch (IOException ioexcp) {
            ioexcp.printStackTrace();
        }

        Assert.fail();
    }

    @Test
    public void testCreateAndDropTable1() {
        try {
            HBaseConfig config = new HBaseConfig.Builder()
                    .setQuorum(Sample.ZKQuorum)
                    .setDistributedCluster(true)
                    .setZNodeParent(Sample.ZKParent)
                    .build();
            HBaseBasicController controller = new HBaseBasicController(config);

            try {
                if (controller.existsTable(Sample.Tablename)) {
                    controller.dropTable(Sample.Tablename);
                }

                controller.createTable(Sample.Tablename, new String[] { Sample.ColumnFamily1, Sample.ColumnFamily2 });
                Assert.assertTrue(controller.existsTable(Sample.Tablename));

                controller.dropTable(Sample.Tablename);
                Assert.assertFalse(controller.existsTable(Sample.Tablename));

                return;
            }
            finally {
                controller.close();
            }
        }
        catch (IOException ioexcp) {
            ioexcp.printStackTrace();
        }

        Assert.fail();
    }

    @Test
    public void testCreateAndDropTable2() {
        try {
            HBaseConfig config = new HBaseConfig.Builder()
                    .setQuorum(Sample.ZKQuorum)
                    .setDistributedCluster(true)
                    .setZNodeParent(Sample.ZKParent)
                    .build();
            HBaseBasicController controller = new HBaseBasicController(config);
            Admin admin = controller.getAdmin();

            try {
                Assert.assertNotNull(admin);

                if (controller.existsTable(admin, Sample.Tablename)) {
                    controller.dropTable(admin, Sample.Tablename);
                }

                controller.createTable(admin, Sample.Tablename, new String[] { Sample.ColumnFamily1, Sample.ColumnFamily2 });
                Assert.assertTrue(controller.existsTable(admin, Sample.Tablename));

                controller.dropTable(admin, Sample.Tablename);
                Assert.assertFalse(controller.existsTable(admin, Sample.Tablename));

                return;
            }
            finally {
                admin.close();
                controller.close();
            }
        }
        catch (IOException ioexcp) {
            ioexcp.printStackTrace();
        }

        Assert.fail();
    }

    @Test
    public void testPutGet() {
        try {
            HBaseConfig config = new HBaseConfig.Builder()
                    .setQuorum(Sample.ZKQuorum)
                    .setDistributedCluster(true)
                    .setZNodeParent(Sample.ZKParent)
                    .build();
            HBaseBasicController controller = new HBaseBasicController(config);
            Admin admin = controller.getAdmin();

            boolean didCreateTable = false;

            try {
                if (!controller.existsTable(admin, Sample.Tablename)) {
                    controller.createTable(admin, Sample.Tablename, new String[] { Sample.ColumnFamily1, Sample.ColumnFamily2 });
                    didCreateTable = true;
                }

                Result result = controller.get(Sample.Tablename, new String(Sample.RowKey).getBytes());
                Assert.assertNotNull(result);

                return;
            }
            finally {
                if (didCreateTable) {
                    controller.dropTable(admin, Sample.Tablename);
                }

                admin.close();
                controller.close();
            }
        }
        catch (IOException ioexcp) {
            ioexcp.printStackTrace();
        }

        Assert.fail();
    }

    @Test
    public void testPutGetScan() {
        try {
            HBaseConfig config = new HBaseConfig.Builder()
                    .setQuorum(Sample.ZKQuorum)
                    .setDistributedCluster(true)
                    .setZNodeParent(Sample.ZKParent)
                    .build();
            HBaseBasicController controller = new HBaseBasicController(config);
            Admin admin = controller.getAdmin();

            boolean didCreateTable = false;

            try {
                if (!controller.existsTable(admin, Sample.Tablename)) {
                    controller.createTable(admin, Sample.Tablename, new String[] { Sample.ColumnFamily1, Sample.ColumnFamily2 });
                    didCreateTable = true;
                }

                byte[][] families = new byte[][] {
                        Sample.ColumnFamily1.getBytes(),
                        Sample.ColumnFamily1.getBytes(),
                        Sample.ColumnFamily1.getBytes(),
                        Sample.ColumnFamily2.getBytes(),
                        Sample.ColumnFamily2.getBytes(),
                        Sample.ColumnFamily2.getBytes()};
                byte[][] columns = new byte[][] {
                        Sample.Column1_1.getBytes(),
                        Sample.Column1_2.getBytes(),
                        Sample.Column1_3.getBytes(),
                        Sample.Column2_1.getBytes(),
                        Sample.Column2_2.getBytes(),
                        Sample.Column2_3.getBytes()};
                byte[][] values = new byte[][] {
                        Sample.ColumnValue1_1.getBytes(),
                        Sample.ColumnValue1_2.getBytes(),
                        Sample.ColumnValue1_3.getBytes(),
                        ByteBuffer.allocate(4).putInt(Sample.ColumnValue2_1).array(),
                        ByteBuffer.allocate(4).putInt(Sample.ColumnValue2_2).array(),
                        ByteBuffer.allocate(4).putInt(Sample.ColumnValue2_3).array()};

                controller.put(Sample.Tablename, Sample.RowKey.getBytes(), families, columns, values);

                //  scan
                {
                    List<Result> results = controller.scan(
                            Sample.Tablename,
                            new byte[][]{Sample.ColumnFamily1.getBytes(), Sample.ColumnFamily1.getBytes(), Sample.ColumnFamily1.getBytes(), Sample.ColumnFamily2.getBytes(), Sample.ColumnFamily2.getBytes(), Sample.ColumnFamily2.getBytes()},
                            new byte[][]{Sample.Column1_1.getBytes(), Sample.Column1_2.getBytes(), Sample.Column1_3.getBytes(), Sample.Column2_1.getBytes(), Sample.Column2_2.getBytes(), Sample.Column2_3.getBytes()});

                    Assert.assertNotNull(results);

                    boolean checkResult = false;

                    for (Result result : results) {
                        byte[] rowKey = result.getRow();
                        String strRowKey = new String(rowKey, "UTF-8");

                        if (strRowKey.equals(Sample.RowKey)) {
                            String value1 = new String(result.getValue(Sample.ColumnFamily1.getBytes(), Sample.Column1_1.getBytes()), "UTF-8");
                            String value2 = new String(result.getValue(Sample.ColumnFamily1.getBytes(), Sample.Column1_2.getBytes()), "UTF-8");
                            String value3 = new String(result.getValue(Sample.ColumnFamily1.getBytes(), Sample.Column1_3.getBytes()), "UTF-8");

                            System.out.printf("Checking 'column1' ... (ref : %s, result : %s).\n", Sample.ColumnValue1_1, value1);
                            Assert.assertEquals(Sample.ColumnValue1_1, value1);
                            System.out.printf("Checking 'column1' ... (ref : %s, result : %s).\n", Sample.ColumnValue1_2, value2);
                            Assert.assertEquals(Sample.ColumnValue1_2, value2);
                            System.out.printf("Checking 'column1' ... (ref : %s, result : %s).\n", Sample.ColumnValue1_3, value3);
                            Assert.assertEquals(Sample.ColumnValue1_3, value3);

                            checkResult = true;
                        }
                    }

                    Assert.assertTrue("Checking 'scan' ... result : [failed].", checkResult);
                }

                //  get
                {
                    Result result = controller.get(Sample.Tablename, Sample.RowKey.getBytes());

                    byte[] row = result.getRow();
                    Assert.assertNotNull(String.format("There must exists row for rowKey(%s). ", Sample.RowKey) , row);

                    byte[] value1 = result.getValue(Sample.ColumnFamily1.getBytes(), Sample.Column1_1.getBytes());
                    byte[] value2 = result.getValue(Sample.ColumnFamily1.getBytes(), Sample.Column1_2.getBytes());
                    byte[] value3 = result.getValue(Sample.ColumnFamily1.getBytes(), Sample.Column1_3.getBytes());
                    byte[] value4 = result.getValue(Sample.ColumnFamily2.getBytes(), Sample.Column2_1.getBytes());
                    byte[] value5 = result.getValue(Sample.ColumnFamily2.getBytes(), Sample.Column2_2.getBytes());
                    byte[] value6 = result.getValue(Sample.ColumnFamily2.getBytes(), Sample.Column2_3.getBytes());

                    //  check if result contains values
                    Assert.assertNotNull(String.format("It must be not null (column := {family : %s, qualifier : %s}", Sample.ColumnFamily1, Sample.Column1_1), value1);
                    Assert.assertNotNull(String.format("It must be not null (column := {family : %s, qualifier : %s}", Sample.ColumnFamily1, Sample.Column1_2), value2);
                    Assert.assertNotNull(String.format("It must be not null (column := {family : %s, qualifier : %s}", Sample.ColumnFamily1, Sample.Column1_3), value3);
                    Assert.assertNotNull(String.format("It must be not null (column := {family : %s, qualifier : %s}", Sample.ColumnFamily1, Sample.Column1_1), value1);
                    Assert.assertNotNull(String.format("It must be not null (column := {family : %s, qualifier : %s}", Sample.ColumnFamily1, Sample.Column1_2), value2);
                    Assert.assertNotNull(String.format("It must be not null (column := {family : %s, qualifier : %s}", Sample.ColumnFamily1, Sample.Column1_3), value3);

                    String columnValue1 = new String(value1, "UTF-8");
                    String columnValue2 = new String(value2, "UTF-8");
                    String columnValue3 = new String(value3, "UTF-8");
                    int columnValue4 = ByteBuffer.wrap(value4).getInt();
                    int columnValue5 = ByteBuffer.wrap(value5).getInt();
                    int columnValue6 = ByteBuffer.wrap(value6).getInt();

                    //  check if the result values are equal to the original values
                    Assert.assertEquals(Sample.ColumnValue1_1, columnValue1);
                    Assert.assertEquals(Sample.ColumnValue1_2, columnValue2);
                    Assert.assertEquals(Sample.ColumnValue1_3, columnValue3);
                    Assert.assertEquals(Sample.ColumnValue2_1, columnValue4);
                    Assert.assertEquals(Sample.ColumnValue2_2, columnValue5);
                    Assert.assertEquals(Sample.ColumnValue2_3, columnValue6);
                }

                return;
            }
            finally {
                if (didCreateTable) {
                    controller.dropTable(admin, Sample.Tablename);
                }

                admin.close();
                controller.close();
            }
        }
        catch (IOException ioexcp) {
            ioexcp.printStackTrace();
        }

        Assert.fail();
    }
}
