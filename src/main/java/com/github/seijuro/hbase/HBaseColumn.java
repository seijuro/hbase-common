package com.github.seijuro.hbase;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class HBaseColumn extends HBaseColumnFamily {
    public static List<HBaseColumn> asList(byte[][] families, byte[][] columns) {
        List<HBaseColumn> results = new ArrayList<>();

        for (int index = 0; index != families.length; ++index) {
            results.add(new HBaseColumn(families[index], columns[index]));
        }

        return results;
    }

    /**
     * Instance Properties
     */
    @Getter
    private final String qualifierName;

    /**
     * C'tor
     *
     * @param $family
     * @param $qualifier
     */
    public HBaseColumn(String $family, String $qualifier) {
        super($family);

        this.qualifierName = $qualifier;
    }

    /**
     * C'tor
     *
     * @param $family
     * @param $qualifier
     */
    public HBaseColumn(byte[] $family, byte[] $qualifier) {
        super($family);

        this.qualifierName = Bytes.toString($qualifier);
    }

    /**
     * return column qualifier as bytes array
     * @return
     */
    public byte[] toBytesQualifier() {
        return Bytes.toBytes(this.qualifierName);
    }
}
