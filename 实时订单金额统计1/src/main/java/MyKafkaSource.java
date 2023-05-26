import com.esotericsoftware.minlog.Log;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 5/24/2023 3:02 PM
 * @desc: 暂未测试
 **/
public class MyKafkaSource implements SourceFunction<OrderDTO> {

    private static final long serialVersionUID = -9171435840423368693L;
    private final transient KafkaConsumer<String, String> consumer;
    private volatile transient boolean cancel;

    private final static Logger log = LoggerFactory.getLogger(MyKafkaSource.class);

    public MyKafkaSource(Collection<String> topics) throws IOException {

        cancel = false;
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("topics can not be null");
        }

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("kafkaConfig.properties");
        if (is == null) {
            throw new IllegalArgumentException("can not read kafka config");
        }
        Properties props = new Properties();
        props.load(is);
        if (props.isEmpty()) {
            throw new IllegalArgumentException("can not parse kafka config");
        }
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
    }

    public void run(SourceContext<OrderDTO> context) throws Exception {


        while (!cancel && consumer != null) {
            ConsumerRecords<String, String> data = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            for (ConsumerRecord<String, String> item : data) {
                try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(item.value().getBytes(StandardCharsets.UTF_8)))) {
                    OrderDTO orderDTO = (OrderDTO) ois.readObject();
                    context.collect(orderDTO);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }

    }


    public void cancel() {
        cancel = true;
    }
}
