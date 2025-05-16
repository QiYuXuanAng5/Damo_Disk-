package Tiered_Processing;

import bean.DimBaseCategory;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import func.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.time.Time;
import utils.*;

import java.sql.Connection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Package Tiered_Processing.OrderInfoAndDetail
 * @Author guo.jia.hui
 * @Date 2025/5/15 22:36
 * @description:
 */
public class OrderInfoAndDetail {
    private static final List<DimBaseCategory> dim_base_categories;

    private static final Connection connection;
    private static final double time_rate_weight_coefficient = 0.1;    // 时间权重系数
    private static final double amount_rate_weight_coefficient = 0.15;    // 价格权重系数
    private static final double brand_rate_weight_coefficient = 0.2;    // 品牌权重系数
    private static final double category_rate_weight_coefficient = 0.3; // 类目权重系数

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

        // user_info处理
        SingleOutputStreamOperator<String> kafkaCdcDb = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        Config.KAFKA_BOOT_SERVER,
                        "disk_data",
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkUtil.WatermarkStrategy(),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");

        //将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> dataConvertJsonDs = kafkaCdcDb.map(JSON::parseObject)
                .uid("convert json cdc db")
                .name("convert json cdc db");
        //dataConvertJsonDs.print();

        SingleOutputStreamOperator<JSONObject> cdcOrderInfoDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("order_info"))
                .uid("filter kafka order info")
                .name("filter kafka order info");
        SingleOutputStreamOperator<JSONObject> cdcOrderDetailDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"))
                .uid("filter kafka order detail")
                .name("filter kafka order detail");

        //cdcOrderInfoDs.print("order_info");
        SingleOutputStreamOperator<JSONObject> mapCdcOrderInfoDs = cdcOrderInfoDs.map(new MapOrderInfoDataFunc()).filter(obj -> !obj.isEmpty());
        //mapCdcOrderInfoDs.print("mapCdcOrderInfoDs");
        SingleOutputStreamOperator<JSONObject> mapCdcOrderDetailDs = cdcOrderDetailDs.map(new MapOrderDetailFunc()).filter(obj -> !obj.isEmpty());

        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderInfoDs = mapCdcOrderInfoDs.filter(data -> data.getString("id") != null && !data.getString("id").isEmpty());
        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderDetailDs = mapCdcOrderDetailDs.filter(data -> data.getString("order_id") != null && !data.getString("order_id").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamCdcOrderInfoDs = filterNotNullCdcOrderInfoDs.keyBy(data -> data.getString("id"));
        KeyedStream<JSONObject, String> keyedStreamCdcOrderDetailDs = filterNotNullCdcOrderDetailDs.keyBy(data -> data.getString("order_id"));

        SingleOutputStreamOperator<JSONObject> processIntervalJoinOrderInfoAndDetailDs = keyedStreamCdcOrderInfoDs.intervalJoin(keyedStreamCdcOrderDetailDs)
                .between(Time.minutes(-2), Time.minutes(2))
                .process(new IntervalDbOrderInfoJoinOrderDetailProcessFunc());
        //processIntervalJoinOrderInfoAndDetailDs.print("processIntervalJoinOrderInfoAndDetailDs");

        SingleOutputStreamOperator<JSONObject> processDuplicateOrderInfoAndDetailDs = processIntervalJoinOrderInfoAndDetailDs.keyBy(data -> data.getString("detail_id"))
                .process(new processOrderInfoAndDetailFunc());
        //processDuplicateOrderInfoAndDetailDs.print("processDuplicateOrderInfoAndDetailDs");

        SingleOutputStreamOperator<JSONObject> mapOrderInfoAndDetailModelDs = processDuplicateOrderInfoAndDetailDs.map(new MapOrderAndDetailRateModelFunc(dim_base_categories, time_rate_weight_coefficient, amount_rate_weight_coefficient, brand_rate_weight_coefficient, category_rate_weight_coefficient));
        mapOrderInfoAndDetailModelDs.print("OrderInfoAndDetail");

        // 1. 定义FileSink
        FileSink<String> csvSink = FileSink
                .forRowFormat(new Path("demo_disk/output/order_detail"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024) // 1GB
                                .build())
                .build();

        // 2. 转换JSON为CSV格式
        SingleOutputStreamOperator<String> csvOutput = mapOrderInfoAndDetailModelDs
                .map(json -> {
                    // 构建CSV头部（可选，如果需要在每个文件开头添加头部）
                    // 这里我们选择不添加头部，因为Flink会生成多个文件

                    // 构建CSV行数据
                    return String.join(",",
                            "\"" + json.getString("order_id") + "\"",
                            "\"" + json.getString("detail_id") + "\"",
                            "\"" + json.getString("uid") + "\"",
                            "\"" + json.getString("b1_name") + "\"",
                            "\"" + json.getString("tname") + "\"",
                            "\"" + json.getString("sku_name") + "\"",
                            json.getString("order_price"),
                            json.getString("sku_num"),
                            json.getString("original_total_amount"),
                            json.getString("total_amount"),
                            json.getString("split_total_amount"),
                            json.getString("split_activity_amount"),
                            json.getString("split_coupon_amount"),
                            "\"" + json.getString("pay_time_slot") + "\"",
                            json.getString("create_time"),
                            json.getString("ts_ms"),
                            json.getString("province_id"),
                            json.getString("c3id"),
                            json.getString("consignee"),
                            json.getString("sku_id"),
                            json.getString("b1name_18-24"),
                            json.getString("b1name_25-29"),
                            json.getString("b1name_30-34"),
                            json.getString("b1name_35-39"),
                            json.getString("b1name_40-49"),
                            json.getString("b1name_50"),
                            json.getString("tname_18-24"),
                            json.getString("tname_25-29"),
                            json.getString("tname_30-34"),
                            json.getString("tname_35-39"),
                            json.getString("tname_40-49"),
                            json.getString("tname_50"),
                            json.getString("amount_18-24"),
                            json.getString("amount_25-29"),
                            json.getString("amount_30-34"),
                            json.getString("amount_35-39"),
                            json.getString("amount_40-49"),
                            json.getString("amount_50"),
                            json.getString("pay_time_18-24"),
                            json.getString("pay_time_25-29"),
                            json.getString("pay_time_30-34"),
                            json.getString("pay_time_35-39"),
                            json.getString("pay_time_40-49"),
                            json.getString("pay_time_50")
                    ) + "\n";
                });

        // 3. 输出到CSV文件
        csvOutput.sinkTo(csvSink).name("order_detail_csv_sink");

        //将数据转换为字符串并上传到kafka中
        //        SingleOutputStreamOperator<String> model_ds = mapOrderInfoAndDetailModelDs.map((MapFunction<JSONObject, String>) JSONAware::toJSONString);
        //        KafkaSink<String> damo_disk_four = KafkaUtil.getKafkaProduct(Config.KAFKA_BOOT_SERVER, "damo_disk_three");
        //        model_ds.sinkTo(damo_disk_four);

        env.execute();
    }
}
