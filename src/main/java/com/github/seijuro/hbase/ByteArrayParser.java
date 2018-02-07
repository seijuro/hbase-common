package com.github.seijuro.hbase;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * (Helper) conversion from byte[] to primitive types, and vice versa.
 */
public class ByteArrayParser {
    private static final String DefaultCharsetName = "UTF-8";


    /**
     * convert byte[] into integer.
     *
     * @param bytes
     * @return
     */
    public static int parseInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    /**
     * convert byte[] into short.
     *
     * @param bytes
     * @return
     */
    public static short parseShort(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getShort();
    }

    /**
     * convert byte[] into long.
     *
     * @param bytes
     * @return
     */
    public static long parseLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    /**
     * convert byte[] into float.
     *
     * @param bytes
     * @return
     */
    public static float parseFloat(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getFloat();
    }

    /**
     * convert byte[] into double.
     *
     * @param bytes
     * @return
     */
    public static double parseDouble(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getDouble();
    }

    /**
     * convert byte[] into String
     *
     * @param bytes
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String parseString(byte[] bytes) throws UnsupportedEncodingException {
        return parseString(bytes, DefaultCharsetName);
    }

    /**
     * convert byte[] into String
     *
     * @param bytes
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String parseString(byte[] bytes, String charsetName) throws UnsupportedEncodingException {
        return new String(bytes, charsetName);
    }

    /**
     * convert integer value to byte[]
     *
     * @param value
     * @return
     */
    public static byte[] parseByteArray(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    /**
     * convert short value to byte[]
     *
     * @param value
     * @return
     */
    public static byte[] parseByteArray(short value) {
        return ByteBuffer.allocate(Short.BYTES).putShort(value).array();
    }

    /**
     * convert long value to byte[]
     *
     * @param value
     * @return
     */
    public static byte[] parseByteArray(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    /**
     * convert float value to byte[]
     *
     * @param value
     * @return
     */
    public static byte[] parseByteArray(float value) {
        return ByteBuffer.allocate(Float.BYTES).putFloat(value).array();
    }

    /**
     * convert double value to byte[]
     *
     * @param value
     * @return
     */
    public static byte[] parseByteArray(double value) {
        return ByteBuffer.allocate(Double.BYTES).putDouble(value).array();
    }

    /**
     * convert String instance to byte[]
     *
     * @param text
     * @return
     */
    public static byte[] parseByteArray(String text) {
        return text.getBytes();
    }
}
