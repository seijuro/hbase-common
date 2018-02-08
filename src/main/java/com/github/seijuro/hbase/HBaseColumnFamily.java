package com.github.seijuro.hbase;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode
@ToString
public class HBaseColumnFamily {
    public static List<HBaseColumnFamily> asList(byte[][] families) {
        List<HBaseColumnFamily> results = new ArrayList<>();

        for (int index = 0; index != families.length; ++index) {
            results.add(new HBaseColumnFamily(families[index]));
        }

        return results;
    }

    /**
     * Instance Properties
     */
    @Getter
    protected final String columnFamilyName;

    /**
     * C'tor
     *
     * @param $name : String
     */
    public HBaseColumnFamily(String $name) {
        this.columnFamilyName = $name;
    }

    /**
     * C'tor
     *
     * @param $name : byte[]
     */
    public HBaseColumnFamily(byte[] $name) {
        this.columnFamilyName = Bytes.toString($name);
    }

    /**
     * convert name to byte[]
     *
     * @return
     */
    public byte[] toBytesFamily() {
        return Bytes.toBytes(this.columnFamilyName);
    }
}
