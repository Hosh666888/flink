import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 5/24/2023 3:02 PM
 * @desc:
 **/
public class MyKafkaSource implements SourceFunction<OrderDTO> {

    private final KafkaConsumer<String, OrderDTO> consumer;
    private volatile boolean cancel;

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
        consumer = new KafkaConsumer<String, OrderDTO>(props);
        consumer.subscribe(topics);
    }

    public void run(SourceContext<OrderDTO> context) throws Exception {

        while (!cancel && consumer != null) {

            ConsumerRecords<String, OrderDTO> data = consumer.poll(100);
            for (ConsumerRecord<String, OrderDTO> item : data) {
                OrderDTO value = item.value();
                if (value != null) {
                    context.collect(value);
                }
            }
        }

    }

    public void cancel() {
        cancel = true;
    }
}
