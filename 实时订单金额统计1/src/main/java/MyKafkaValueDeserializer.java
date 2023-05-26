import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.JavaObjectInputStreamReadString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 5/25/2023 5:19 PM
 * @desc:
 **/
public class MyKafkaValueDeserializer implements DeserializationSchema<OrderDTO> {

    private final static Logger log = LoggerFactory.getLogger(MyKafkaValueDeserializer.class);

    @Override
    public TypeInformation<OrderDTO> getProducedType() {
        return TypeInformation.of(OrderDTO.class);
    }


    @Override
    public OrderDTO deserialize(byte[] bytes) {
        log.info("接收到了:"+bytes.length);

        try (final ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
             final ObjectInputStream ois = new ObjectInputStream(inputStream)
        ){
            return (OrderDTO) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            log.error(e.getMessage(),e);
        }

        return null;
    }

    @Override
    public boolean isEndOfStream(OrderDTO orderDTO) {
        return false;
    }
}
