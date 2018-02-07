package com.github.seijuro.hbase.util;

import com.github.seijuro.hbase.util.ByteArrayParser;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class ByteArrayParserTest {
    @Test
    public void testInt() {
        int testValue = 100;
        byte[] ref = ByteBuffer.allocate(Integer.BYTES).putInt(testValue).array();

        byte[] byteArrayValue = ByteArrayParser.parseByteArray(testValue);

        Assert.assertNotNull(byteArrayValue);
        Assert.assertEquals(byteArrayValue.length, ref.length);

        for (int index = 0; index != ref.length; ++index) {
            Assert.assertEquals(ref[index], byteArrayValue[index]);
        }

        int intValue = ByteArrayParser.parseInt(byteArrayValue);

        Assert.assertEquals(testValue, intValue);
    }

    @Test
    public void testShort() {
        short testValue = 1;
        byte[] ref = ByteBuffer.allocate(Short.BYTES).putShort(testValue).array();

        byte[] byteArrayValue = ByteArrayParser.parseByteArray(testValue);

        Assert.assertNotNull(byteArrayValue);
        Assert.assertEquals(byteArrayValue.length, ref.length);

        for (int index = 0; index != ref.length; ++index) {
            Assert.assertEquals(ref[index], byteArrayValue[index]);
        }

        short shortValue = ByteArrayParser.parseShort(byteArrayValue);

        Assert.assertEquals(testValue, shortValue);
    }

    @Test
    public void testLong() {
        long testValue = 1000000L;
        byte[] ref = ByteBuffer.allocate(Long.BYTES).putLong(testValue).array();

        byte[] byteArrayValue = ByteArrayParser.parseByteArray(testValue);

        Assert.assertNotNull(byteArrayValue);
        Assert.assertEquals(byteArrayValue.length, ref.length);

        for (int index = 0; index != ref.length; ++index) {
            Assert.assertEquals(ref[index], byteArrayValue[index]);
        }

        long longValue = ByteArrayParser.parseLong(byteArrayValue);

        Assert.assertEquals(testValue, longValue);
    }

    @Test
    public void testFloat() {
        float testValue = 100f;
        byte[] ref = ByteBuffer.allocate(Float.BYTES).putFloat(testValue).array();

        byte[] byteArrayValue = ByteArrayParser.parseByteArray(testValue);

        Assert.assertNotNull(byteArrayValue);
        Assert.assertEquals(byteArrayValue.length, ref.length);

        for (int index = 0; index != ref.length; ++index) {
            Assert.assertEquals(ref[index], byteArrayValue[index]);
        }

        float floatValue = ByteArrayParser.parseFloat(byteArrayValue);

        Assert.assertTrue(Float.compare(testValue, floatValue) == 0);
    }

    @Test
    public void testDouble() {
        double testValue = 100f;
        byte[] ref = ByteBuffer.allocate(Double.BYTES).putDouble(testValue).array();

        byte[] byteArrayValue = ByteArrayParser.parseByteArray(testValue);

        Assert.assertNotNull(byteArrayValue);
        Assert.assertEquals(byteArrayValue.length, ref.length);

        for (int index = 0; index != ref.length; ++index) {
            Assert.assertEquals(ref[index], byteArrayValue[index]);
        }

        double doubleValue = ByteArrayParser.parseDouble(byteArrayValue);

        Assert.assertTrue(Double.compare(testValue, doubleValue) == 0);
    }
}
