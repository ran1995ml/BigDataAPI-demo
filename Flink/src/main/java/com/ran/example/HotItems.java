package com.ran.example;

import com.ran.domain.ItemViewCount;
import com.ran.domain.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * ClassName: HotItems
 * Description:统计热门商品TopN
 * 过滤出点击行为数据，窗口大小1小时，每5分钟聚合一次
 * date: 2022/3/13 17:30
 *
 * @author ran
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String path = "D:\\Users\\ran\\IdeaProjects\\BigDataAPI-demo\\Flink\\src\\main\\java\\com\\ran\\file\\UserBehavior.csv";
        DataStreamSource<String> inputStream = env.readTextFile(path);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //转化为PoJo，分配时间戳和watermark
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] split = s.split(",");
                long userId = Long.parseLong(split[0]);
                long itemId = Long.parseLong(split[1]);
                long categoryId = Long.parseLong(split[2]);
                String behavior = split[3];
                long timestamp = Long.parseLong(split[4]);
                return new UserBehavior(userId, itemId, categoryId, behavior, timestamp);
            }//数据是正序的，每条数据的eventtime都可作为watermark
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                return userBehavior.getTimestamp() * 1000L;
            }
        });

        SingleOutputStreamOperator<ItemViewCount> windowStream = dataStream.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.getBehavior().equals("pv");
            }
        })
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemsAggr(), new WindowResult());
        windowStream.keyBy("windowEnd").process(new TopKResult(3)).print();

        env.execute();
    }


    private static class ItemsAggr implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }


    private static class WindowResult implements WindowFunction<Long,ItemViewCount,Tuple,TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            long itemId = tuple.getField(0);
            long windowEnd = timeWindow.getEnd();
            long count = iterable.iterator().next();
            collector.collect(new ItemViewCount(itemId,windowEnd,count));
        }
    }

    private static class TopKResult extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private int k;
        private ListState<ItemViewCount> state;

        public TopKResult(int k){
            this.k = k;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-count-list",ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            state.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<ItemViewCount> list = Lists.newArrayList(state.get().iterator());
            list.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.getCount() - o1.getCount());
                }
            });
            StringBuilder sb = new StringBuilder();
            sb.append("窗口结束时间:").append(new Timestamp(timestamp-1)).append("\n");
            for(int i=0;i<Math.min(k, list.size());i++){
                ItemViewCount itemViewCount = list.get(i);
                sb.append("No.").append(i+1).append(":").append(itemViewCount.getItemId()).append("\n");
            }
            Thread.sleep(1000);
            out.collect(sb.toString());
        }
    }
}
