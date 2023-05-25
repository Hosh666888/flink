import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.tools.nsc.Main;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.util.Properties;
import java.util.UUID;

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 5/25/2023 9:47 AM
 * @desc:
 **/
public class Test {
    public static void main(String[] args) throws IOException, InterruptedException {

        InputStream resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("kafkaConfig.properties");
        Properties properties = new Properties();
        properties.load(resourceAsStream);

        KafkaProducer<String, OrderDTO> producer = new KafkaProducer<>(properties, new StringSerializer(), (s, orderDTO) -> orderDTO.toString().getBytes(StandardCharsets.UTF_8));

        while (true) {
            OrderDTO orderDTO = new OrderDTO();
            orderDTO.createTime = System.currentTimeMillis();
            orderDTO.amount = Math.random() * 2000;
            orderDTO.key = "123";
            orderDTO.orderNum = UUID.randomUUID().toString();

            ProducerRecord<String, OrderDTO> dto = new ProducerRecord<>("orders", orderDTO.orderNum, orderDTO);

            producer.send(dto);

            Thread.sleep(((long) (Math.random() * 600)));

        }


    }
}
