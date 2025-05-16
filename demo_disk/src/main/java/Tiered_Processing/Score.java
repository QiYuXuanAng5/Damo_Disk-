package Tiered_Processing;

import bean.DimBaseCategory;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import func.AggregateUserDataProcessFunction;
import func.MapDevice;
import func.MapDeviceAndSearchMarkModelFunc;
import func.ProcessFilterRepeatTsDataFunc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import utils.*;

import java.sql.Connection;
import java.util.Date;
import java.util.List;

/**
 * @Package Tiered_Processing.Score
 * @Author guo.jia.hui
 * @Date 2025/5/15 22:33
 * @description:
 */
public class Score {
    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数

    private static final List<DimBaseCategory> dim_base_categories;

    private static final Connection connection;

    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    "jdbc:mysql://cdh03:3306/flink_realtime",
                    "root",
                    "root");
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from flink_realtime.base_category3 as b3  \n" +
                    "     join flink_realtime.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join flink_realtime.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> kafkaPageLogSource = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                "cdh01:9092",
                                "topic_log",
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkUtil.WatermarkStrategy(),
                        "kafka_page_log_source"
                ).uid("kafka_page_log_source")
                .name("kafka_page_log_source");

        SingleOutputStreamOperator<JSONObject> dataPageLogConvertJsonDs = kafkaPageLogSource.map(JSON::parseObject)
                .uid("convert json page log")
                .name("convert json page log");
        //dataPageLogConvertJsonDs.print("dataPageLogConvertJsonDs");

        // 设备信息 + 关键词搜索
        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = dataPageLogConvertJsonDs.map(new MapDevice())
                .uid("get device info & search")
                .name("get device info & search");
        //logDeviceInfoDs.print("设备信息 + 关键词搜索");

        //对数据进行逻辑分区
        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));
        //keyedStreamLogPageMsg.print("逻辑分区");


        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsDataFunc());
        //processStagePageLogDs.print("状态去重");

        // 2 min 分钟窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("win 2 minutes page count msg")
                .name("win 2 minutes page count msg");
        //win2MinutesPageLogsDs.print("2 min 分钟窗口");

        // 设置打分模型
        SingleOutputStreamOperator<JSONObject> mapDeviceAndSearchRateResultDs = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));
        //mapDeviceAndSearchRateResultDs.print("打分模型");

        env.execute();
    }
}
