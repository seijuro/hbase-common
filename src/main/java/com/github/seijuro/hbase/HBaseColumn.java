package com.github.seijuro.hbase;

import lombok.Getter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by myungjoonlee on 2017. 7. 13..
 */
public class HBaseColumn {
    /**
     * Instance Properties
     */
    @Getter
    private final String family;
    @Getter
    private final String qualifier;

    /**
     * C'tor
     *
     * @param f
     * @param q
     */
    public HBaseColumn(String f, String q) {
        this.family = f;
        this.qualifier = q;
    }

    /**
     * return column family as bytes array
     * @return
     */
    public byte[] familtyBytes() {
        return Bytes.toBytes(this.family);
    }

    /**
     * return column qualifier as bytes array
     * @return
     */
    public byte[] qualifierBytes() {
        return Bytes.toBytes(this.qualifier);
    }

    /**
     * override hashCode to use <code>HBaseColumn</code> instance with some datat structure(s), such as <code>HaspMap</code>.
     *
     * @return
     */
    @Override
    public int hashCode() {
        int hash = 17;

        return (family.hashCode() * hash) << 31 + this.qualifier.hashCode();
    }

    /**
     * return true, if it's properties has same values with rhs's properties.
     * override equals to use <code>HBaseColumn</code> instance with some datat structure(s), such as <code>HaspMap</code>.
     *
     * @return
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HBaseColumn) {
            HBaseColumn rhs = (HBaseColumn)obj;
            return this.family.equals(rhs.family) && this.qualifier.equals(rhs.qualifier);
        }

        return false;
    }
}
