import bean.UserInfo;
import bean.UserInfoSup;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import utils.Config;
import utils.KafkaUtil;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/**
 * @Package utils.Damo_Disk2
 * @Author guo.jia.hui
 * @Date 2025/5/13 8:45
 * @description:
 */
public class Damo_Disk2 {

    public static void main(String[] args) throws Exception {
        // 1. 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

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

                        // 年龄分组 (根据生日计算)
                        long birthdayTimestamp = value.f0.getBirthday();
                        LocalDate birth_day = Instant.ofEpochMilli(birthdayTimestamp)
                                .atZone(ZoneId.systemDefault())
                                .toLocalDate();
                        int age = LocalDate.now().getYear() - birth_day.getYear();
                        String ageGroup = getAgeGroup(age);
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
                        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
                        else if (month == 1 || month == 2 && day <= 18) return "水瓶座";
                        else if (month == 2 || month == 3 && day <= 20) return "双鱼座";
                        else if (month == 3 || month == 4 && day <= 19) return "白羊座";
                        else if (month == 4 || month == 5 && day <= 20) return "金牛座";
                        else if (month == 5 || month == 6 && day <= 21) return "双子座";
                        else if (month == 6 || month == 7 && day <= 22) return "巨蟹座";
                        else if (month == 7 || month == 8 && day <= 22) return "狮子座";
                        else if (month == 8 || month == 9 && day <= 22) return "处女座";
                        else if (month == 9 || month == 10 && day <= 23) return "天秤座";
                        else if (month == 10 || month == 11 && day <= 22) return "天蝎座";
                        else return "射手座";
                    }
                })
                .name("Convert to DMP Tags");

        // 5. 输出结果
        dmpTagsStream.print("DMP Tags");

        // 将数据存储到kafka中
        KafkaSink<String> kafkaSink2 = KafkaUtil.getKafkaProduct(Config.KAFKA_BOOT_SERVER, Config.KAFKA_TOPIC);
        SingleOutputStreamOperator<String> dmpTagsStream2 = dmpTagsStream.map((MapFunction<Map<String, String>, String>) JSONObject::toJSONString);
        dmpTagsStream2.sinkTo(kafkaSink2);

        env.execute("Optimized User Tags Processing");
    }
}
