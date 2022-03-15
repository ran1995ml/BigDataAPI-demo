package com.ran.tableapi.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * ClassName: MyHashCode
 * Description:
 * date: 2022/3/12 16:55
 *
 * @author ran
 */
public class MyHashCode extends ScalarFunction {
    private int factor = 13;

    public MyHashCode(int factor) {
        this.factor = factor;
    }

    public int eval(String s) {
        return s.hashCode() * 13;
    }
}
