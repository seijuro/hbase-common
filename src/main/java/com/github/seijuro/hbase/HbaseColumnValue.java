package com.github.seijuro.hbase;

import lombok.Getter;

/**
 * Created by myungjoonlee on 2017. 7. 13..
 */
public class HbaseColumnValue {
    @Getter
    private final HBaseColumn column;
    @Getter
    private byte[] value;

    public HbaseColumnValue(HBaseColumn $column, byte[] $value) {
        this.column = $column;
        this.value = $value;
    }
}
