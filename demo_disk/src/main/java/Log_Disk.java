import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.Config;
import utils.KafkaUtil;

import java.time.Duration;

/**
 * @Package PACKAGE_NAME.Log_Disk
 * @Author guo.jia.hui
 * @Date 2025/5/13 16:18
 * @description: 处理日志数据
 */
public class Log_Disk {
    public static void main(String[] args) throws Exception {
        // 初始化 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取 Kafka 数据源
        DataStreamSource<String> topic_log = KafkaUtil.getKafkaConsumer(env, Config.KAFKA_BOOT_SERVER, "topic_log");

        // 打印 Kafka 数据
        //topic_log.print();

        SingleOutputStreamOperator<JSONObject> Topic_log_json = topic_log.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSONObject.parseObject(s);
            }
        });

        //基础标签topic的数据
        DataStreamSource<String> disk = KafkaUtil.getKafkaConsumer(env, Config.KAFKA_BOOT_SERVER, "disk");
        SingleOutputStreamOperator<JSONObject> disk_json = disk.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.getLong("ts")));
        disk_json.print();

        SingleOutputStreamOperator<JSONObject> process = Topic_log_json.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject log_disk = new JSONObject();
                JSONObject common = jsonObject.getJSONObject("common");
                String mid = common.getString("mid");
                String uid = common.getString("uid");
                Long ts = jsonObject.getLong("ts");
                log_disk.put("mid", mid);
                if (uid != null) {
                    log_disk.put("uid", uid);
                }
                log_disk.put("ts", ts);
                collector.collect(log_disk);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.getLong("ts")));
        process.print("数据处理");

//        SingleOutputStreamOperator<JSONObject> operator = process
//                .keyBy(json -> json.getString("uid"))
//                .intervalJoin(disk_json.keyBy(json -> json.getString("uid")))
//                .between(Time.minutes(-30), Time.minutes(30))
//                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
//
//                    @Override
//                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
//                        right.putAll(left);
//                        collector.collect(right);
//                    }
//                });
//        operator.print("interval连接");


        // 执行 Flink 任务
        env.execute("Log Disk Processing");
    }
}
