package com.neoremind.mrsample.mapred.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author zhangxu
 */
public class JoinSortingComparator extends WritableComparator {

    public JoinSortingComparator() {
        super(ProductIdKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ProductIdKey first = (ProductIdKey) a;
        ProductIdKey second = (ProductIdKey) b;

        return first.compareTo(second);
    }
}
