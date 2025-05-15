import bean.DimBaseCategory;
import bean.UserInfo;
import bean.UserInfoSup;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import func.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import utils.*;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Package utils.Damo_Disk2
 * @Author guo.jia.hui
 * @Date 2025/5/13 8:45
 * @description:
 */
//.uid()	为算子指定唯一ID，用于状态持久化/恢复
//.name()	为算子设置可读名称，方便在Web UI/日志中识别
public class Damo_Disk2 {
    private static final List<DimBaseCategory> dim_base_categories;

    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数
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
        // 1. 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 配置 KafkaSource
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("user_info")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> userInfoJsonStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        // 用户主信息流处理
        SingleOutputStreamOperator<UserInfo> userInfoStream = userInfoJsonStream
                .process(new ProcessFunction<String, UserInfo>() {
                    @Override
                    public void processElement(String json, Context ctx, Collector<UserInfo> out) {
                        try {
                            JSONObject jsonObj = JSON.parseObject(json);
                            JSONObject after = jsonObj.getJSONObject("after");

                            UserInfo userInfo = new UserInfo();
                            userInfo.setId(after.getLong("id"));
                            userInfo.setLoginName(after.getString("login_name"));
                            userInfo.setName(after.getString("name"));
                            userInfo.setPhone(after.getString("phone_num"));
                            userInfo.setEmail(after.getString("email"));
                            userInfo.setBirthday(after.getLong("birthday"));
                            userInfo.setGender(after.getString("gender"));
                            userInfo.setTsMs(jsonObj.getLong("ts_ms"));

                            out.collect(userInfo);
                        } catch (Exception e) {
                            // 错误处理：记录错误或发送到侧输出流
                            System.err.println("Failed to parse JSON: " + json);
                            e.printStackTrace();
                        }
                    }
                })
                .name("Parse UserInfo JSON")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserInfo>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                                .withTimestampAssigner((event, timestamp) -> event.getTsMs())
                )
                .name("Assign Timestamps & Watermarks");

        //user_info数据输出
        //userInfoStream.print("user_info");

        KafkaSource<String> source2 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("user_info_sup_msg")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> userInfoSupJsonStream = env.fromSource(source2, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<UserInfoSup> userInfoSupStream = userInfoSupJsonStream
                .process(new ProcessFunction<String, UserInfoSup>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, UserInfoSup>.Context context, Collector<UserInfoSup> collector) {
                        JSONObject jsonObject = JSON.parseObject(s);
                        JSONObject after = jsonObject.getJSONObject("after");
                        UserInfoSup userInfoSup = new UserInfoSup();
                        userInfoSup.setUid(after.getLong("uid"));
                        userInfoSup.setGender(after.getString("gender"));
                        userInfoSup.setHeight(after.getString("height"));
                        userInfoSup.setUnitHeight(after.getString("unit_height"));
                        userInfoSup.setWeight(after.getString("weight"));
                        userInfoSup.setUnitWeight(after.getString("unit_weight"));
                        userInfoSup.setTsMs(jsonObject.getLong("ts_ms"));
                        collector.collect(userInfoSup);
                    }
                })
                .name("Parse UserInfoSup JSON")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserInfoSup>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                                .withTimestampAssigner((event, timestamp) -> event.getTsMs())
                );

        //user_info_sup数据输出
        //userInfoSupStream.print("user_info_sup");

        // 3. 数据关联和处理
        DataStream<Tuple2<UserInfo, UserInfoSup>> joinedStream = userInfoStream
                .keyBy(UserInfo::getId)
                .intervalJoin(userInfoSupStream.keyBy(UserInfoSup::getUid))
                .between(Time.minutes(-30), Time.minutes(30))
                .process(new ProcessJoinFunction<UserInfo, UserInfoSup, Tuple2<UserInfo, UserInfoSup>>() {
                    @Override
                    public void processElement(UserInfo left, UserInfoSup right, Context ctx,
                                               Collector<Tuple2<UserInfo, UserInfoSup>> out) {
                        out.collect(Tuple2.of(left, right));
                    }
                });

        // 打印关联结果
        //joinedStream.print("Joined Stream");

        // 4. 转换为达摩盘标签格式
        SingleOutputStreamOperator<Map<String, String>> dmpTagsStream = joinedStream
                .map(new MapFunction<Tuple2<UserInfo, UserInfoSup>, Map<String, String>>() {
                    @Override
                    public Map<String, String> map(Tuple2<UserInfo, UserInfoSup> value) {
                        Map<String, String> tags = new HashMap<>();

                        // 用户ID
                        tags.put("user_id", String.valueOf(value.f0.getId()));
                        tags.put("login_name", value.f0.getLoginName());
                        tags.put("name", value.f0.getName());
                        tags.put("phone", value.f0.getPhone());
                        tags.put("email", value.f0.getEmail());

                        // 年龄分组 (根据生日计算)
                        long birthdayTimestamp = value.f0.getBirthday();
                        // 将生日时间戳转换为LocalDate对象 (LocalDate)，并设置时区为当前时区，提取Date(yyyy-MM-dd)
                        LocalDate birth_day = Instant.ofEpochMilli(birthdayTimestamp)
                                .atZone(ZoneId.systemDefault())
                                .toLocalDate();
                        int age = LocalDate.now().getYear() - birth_day.getYear();
                        String ageGroup = getAgeGroup(age);
                        tags.put("age", String.valueOf(age));
                        tags.put("age_group", ageGroup);

                        // 性别处理 (主信息中gender为null表示家庭用户)
                        String gender = value.f0.getGender() != null ?
                                value.f0.getGender() :
                                (value.f1 != null ? value.f1.getGender() : "home");
                        if (gender == null) {
                            gender = "home"; // 家庭用户
                        }
                        tags.put("gender", gender);

                        // 身高处理 (统一转换为厘米)
                        if (value.f1 != null) {
                            String height = value.f1.getHeight();
                            String unitHeight = value.f1.getUnitHeight();
                            if (height != null && unitHeight != null) {
                                try {
                                    double heightValue = Double.parseDouble(height);
                                    if ("in".equalsIgnoreCase(unitHeight)) {
                                        // 英寸转厘米
                                        heightValue *= 2.54;
                                    } else if ("m".equalsIgnoreCase(unitHeight)) {
                                        // 米转厘米
                                        heightValue *= 100;
                                    }
                                    // 保留1位小数
                                    tags.put("height_cm", String.format("%.1f", heightValue));
                                } catch (NumberFormatException e) {
                                    // 忽略格式错误
                                }
                            }
                        }

                        // 体重处理 (统一转换为千克)
                        if (value.f1 != null) {
                            String weight = value.f1.getWeight();
                            String unitWeight = value.f1.getUnitWeight();
                            if (weight != null && unitWeight != null) {
                                try {
                                    double weightValue = Double.parseDouble(weight);
                                    if ("lb".equalsIgnoreCase(unitWeight)) {
                                        // 磅转千克
                                        weightValue *= 0.453592;
                                    } else if ("g".equalsIgnoreCase(unitWeight)) {
                                        // 克转千克
                                        weightValue /= 1000;
                                    }
                                    // 保留1位小数
                                    tags.put("weight_kg", String.format("%.1f", weightValue));
                                } catch (NumberFormatException e) {
                                    // 忽略格式错误
                                }
                            }
                        }

                        // 星座计算
                        if (birthdayTimestamp != 0) {  // 修改这里，不再检查是否大于0
                            try {
                                LocalDate birthday = Instant.ofEpochMilli(birthdayTimestamp)
                                        .atZone(ZoneId.systemDefault())
                                        .toLocalDate();
                                tags.put("constellation", getConstellation(birthday));
                            } catch (Exception e) {
                                // 处理可能的日期转换异常
                                System.err.println("Error converting timestamp to date: " + birthdayTimestamp);
                            }
                        }
                        tags.put("ts", String.valueOf(value.f0.getTsMs()));

                        return tags;
                    }

                    // 年龄分组逻辑
                    private String getAgeGroup(int age) {
                        if (age < 18) return "under 18";
                        else if (age < 25) return "18-24";
                        else if (age < 30) return "25-29";
                        else if (age < 35) return "30-34";
                        else if (age < 40) return "35-39";
                        else if (age < 50) return "40-49";
                        else return "50+";
                    }

                    // 星座计算逻辑
                    private String getConstellation(LocalDate date) {
                        if (date == null) {
                            return null;
                        }

                        int month = date.getMonthValue();
                        int day = date.getDayOfMonth();

                        // 星座日期范围定义
                        if (month == 1) {
                            return (day <= 19) ? "摩羯座" : "水瓶座";      // 1月1日-1月19日:摩羯座 | 1月20日-1月31日:水瓶座
                        } else if (month == 2) {
                            return (day <= 18) ? "水瓶座" : "双鱼座";      // 2月1日-2月18日:水瓶座 | 2月19日-2月29日:双鱼座
                        } else if (month == 3) {
                            return (day <= 20) ? "双鱼座" : "白羊座";      // 3月1日-3月20日:双鱼座 | 3月21日-3月31日:白羊座
                        } else if (month == 4) {
                            return (day <= 19) ? "白羊座" : "金牛座";      // 4月1日-4月19日:白羊座 | 4月20日-4月30日:金牛座
                        } else if (month == 5) {
                            return (day <= 20) ? "金牛座" : "双子座";      // 5月1日-5月20日:金牛座 | 5月21日-5月31日:双子座
                        } else if (month == 6) {
                            return (day <= 21) ? "双子座" : "巨蟹座";      // 6月1日-6月21日:双子座 | 6月22日-6月30日:巨蟹座
                        } else if (month == 7) {
                            return (day <= 22) ? "巨蟹座" : "狮子座";      // 7月1日-7月22日:巨蟹座 | 7月23日-7月31日:狮子座
                        } else if (month == 8) {
                            return (day <= 22) ? "狮子座" : "处女座";      // 8月1日-8月22日:狮子座 | 8月23日-8月31日:处女座
                        } else if (month == 9) {
                            return (day <= 22) ? "处女座" : "天秤座";      // 9月1日-9月22日:处女座 | 9月23日-9月30日:天秤座
                        } else if (month == 10) {
                            return (day <= 23) ? "天秤座" : "天蝎座";      // 10月1日-10月23日:天秤座 | 10月24日-10月31日:天蝎座
                        } else if (month == 11) {
                            return (day <= 22) ? "天蝎座" : "射手座";      // 11月1日-11月22日:天蝎座 | 11月23日-11月30日:射手座
                        } else {
                            return (day <= 21) ? "射手座" : "摩羯座";      // 12月1日-12月21日:射手座 | 12月22日-12月31日:摩羯座
                        }
                    }
                })
                .name("Convert to DMP Tags");

        // 5. 输出结果
        dmpTagsStream.print("DMP Tags");
        // 6.将数据保存到kafka中
        KafkaSink<String> damo_disk_one = KafkaUtil.getKafkaProduct(Config.KAFKA_BOOT_SERVER, "damo_disk_one");
        dmpTagsStream.map((MapFunction<Map<String, String>, String>) Object::toString).sinkTo(damo_disk_one);

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
        mapDeviceAndSearchRateResultDs.print("打分模型");
        // 将数据转换为字符串并上传到kafka中
        SingleOutputStreamOperator<String> log_score = mapDeviceAndSearchRateResultDs.map((MapFunction<JSONObject, String>) JSONAware::toJSONString);
        KafkaSink<String> damo_disk_two = KafkaUtil.getKafkaProduct(Config.KAFKA_BOOT_SERVER, "damo_disk_two");
        log_score.sinkTo(damo_disk_two);

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

        //将数据转换为字符串并上传到kafka中
        SingleOutputStreamOperator<String> model_ds = mapOrderInfoAndDetailModelDs.map((MapFunction<JSONObject, String>) JSONAware::toJSONString);
        KafkaSink<String> damo_disk_four = KafkaUtil.getKafkaProduct(Config.KAFKA_BOOT_SERVER, "damo_disk_three");
        model_ds.sinkTo(damo_disk_four);

        env.execute("Optimized User Tags Processing");
    }
}
