package com.ran.tableapi.udf;

import com.ran.tableapi.TableUDF;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * ClassName: AvgValue
 * Description:
 * date: 2022/3/12 18:34
 *
 * @author ran
 */
public class AvgValue extends AggregateFunction<Integer, AvgAcc> {



    @Override
    public Integer getValue(AvgAcc avgAcc) {
        return avgAcc.sum/avgAcc.count;
    }

    @Override
    public AvgAcc createAccumulator() {
        return new AvgAcc();
    }

    public void accumulate(AvgAcc acc, Integer temp){
        acc.sum += temp;
        acc.count += 1;
    }
}
class AvgAcc{
    int sum = 0;
    int count = 1;
}
