package com.ran.tableapi.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

/**
 * ClassName: TableFunction
 * Description:表函数，可返回任意数量的行输出
 * date: 2022/3/12 17:11
 *
 * @author ran
 */
public class Split extends TableFunction<Tuple2<String,Integer>> {
    private String separator = ",";

    public Split(String separator) {
        this.separator = separator;
    }
    public void eval(String str){
        for(String s:str.split(separator)){
            collect(new Tuple2<>(s,s.length()));
        }
    }
}
