import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 2023/5/26 15:20
 * @desc:
 **/
public class CanalDemo {
    public static void main(String[] args) throws InvalidProtocolBufferException {

        final CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("127.0.0.1", 11111)
                , "example"
                , "root"
                , "zjj20001031"
        );

        connector.connect();
        connector.subscribe(".*\\..*");
        connector.rollback();

        while (true) {
            final Message message = connector.getWithoutAck(1);
            final List<CanalEntry.Entry> entries = message.getEntries();
            for (CanalEntry.Entry entry : entries) {
                System.out.println("entry.getEntryType() = " + entry.getEntryType());
                final CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());


                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {

                    System.out.println("rowChange.getSql() = " + rowChange.getSql());

                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
                    }
                }
                System.out.println();


            }


        }


    }
}
