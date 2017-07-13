package com.github.seijuro.hbase;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by myungjoonlee on 2017. 7. 13..
 */
public class HBaseColumn {
    private final String family;
    private final String qualifier;

    public HBaseColumn(String f, String q) {
        this.family = f;
        this.qualifier = q;
    }

    public String family() {
        return this.family;
    }

    public String qualifier() {
        return this.qualifier;
    }

    public byte[] familtyBytes() {
        return Bytes.toBytes(this.family);
    }

    public byte[] qualifierBytes() {
        return Bytes.toBytes(this.qualifier);
    }

    @Override
    public int hashCode() {
        int hash = 17;

        return (family.hashCode() * hash) << 31 + this.qualifier.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HBaseColumn) {
            HBaseColumn rhs = (HBaseColumn)obj;
            return this.family.equals(rhs.family) && this.qualifier.equals(rhs.qualifier);
        }

        return false;
    }
}
