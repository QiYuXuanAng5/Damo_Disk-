import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.Config;
import utils.KafkaUtil;

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

        // 获取 Kafka 数据源
        DataStreamSource<String> topic_log = KafkaUtil.getKafkaConsumer(env, Config.KAFKA_BOOT_SERVER, "topic_log");

        // 打印 Kafka 数据
        topic_log.print();

        // 执行 Flink 任务
        env.execute("Log Disk Processing");
    }
}
