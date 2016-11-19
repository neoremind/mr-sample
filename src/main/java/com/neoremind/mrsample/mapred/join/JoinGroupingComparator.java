package com.neoremind.mrsample.mapred.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author zhangxu
 */
public class JoinGroupingComparator  extends WritableComparator {
    public JoinGroupingComparator() {
        super (ProductIdKey.class, true);
    }

    @Override
    public int compare (WritableComparable a, WritableComparable b){
        ProductIdKey first = (ProductIdKey) a;
        ProductIdKey second = (ProductIdKey) b;

        return first.productId.compareTo(second.productId);
    }
}
