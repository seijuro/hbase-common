package com.github.seijuro.hbase;

/**
 * Created by myungjoonlee on 2017. 7. 13..
 */
public class HbaseColumnValue {
    private final HBaseColumn column;
    private byte[] value;

    public HbaseColumnValue(HBaseColumn $column, byte[] $value) {
        this.column = $column;
        this.value = $value;
    }

    public HBaseColumn column() {
        return this.column;
    }

    public byte[] value() {
        return this.value;
    }
}
