package com.github.seijuro.hbase.util;

import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

public class FilterUtils {
    public static class Value {
        public static class SingleColumn {
            public Filter filter(byte[] columnFamily, byte[] column, CompareOp op, byte[] value) {
                Filter filter = new SingleColumnValueFilter(
                        columnFamily,
                        column,
                        op,
                        value);


                return filter;
            }

            public Filter filter(byte[] columnFamily, byte[] column, CompareOp op, ByteArrayComparable comparable) {
                Filter filter = new SingleColumnValueFilter(
                        columnFamily,
                        column,
                        op,
                        comparable);

                return filter;
            }

            public Filter equalFilter(byte[] columnFamily, byte[] column, byte[] value) {
                return filter(columnFamily, column, CompareOp.EQUAL, value);
            }

            public Filter greaterFilter(byte[] columnFamily, byte[] column, byte[] value) {
                return filter(columnFamily, column, CompareOp.GREATER, value);
            }

            public Filter greaterOrEqualFilter(byte[] columnFamily, byte[] column, byte[] value) {
                return filter(columnFamily, column, CompareOp.GREATER_OR_EQUAL, value);
            }

            public Filter lessFilter(byte[] columnFamily, byte[] column, byte[] value) {
                return filter(columnFamily, column, CompareOp.LESS, value);
            }

            public Filter lessOrEqualFilter(byte[] columnFamily, byte[] column, byte[] value) {
                return filter(columnFamily, column, CompareOp.LESS_OR_EQUAL, value);
            }

            public Filter regexStringFilter(byte[] columnFamily, byte[] column, String regex) {
                RegexStringComparator comparator = new RegexStringComparator(regex);

                return filter(columnFamily, column, CompareOp.EQUAL, comparator);
            }

            public Filter substringFilter(byte[] columnFamily, byte[] column, String substr) {
                SubstringComparator comparator = new SubstringComparator(substr);

                return filter(columnFamily, column, CompareOp.EQUAL, comparator);
            }
        }
    }

    public static class Column {
        public Filter prefixFilter(byte[] prefix) {
            Filter filter = new ColumnPrefixFilter(prefix);

            return filter;
        }

        public Filter multiplePrefixFilter(byte[][] prefixs) {
            Filter filter = new MultipleColumnPrefixFilter(prefixs);

            return filter;
        }

        public Filter rangeFilter(byte[] startColumn, byte[] endColumn) {
            Filter filter = new ColumnRangeFilter(startColumn, true, endColumn, true);

            return filter;
        }
    }
}
