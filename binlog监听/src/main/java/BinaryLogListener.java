import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;

import java.sql.Timestamp;

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 2023/5/26 11:08
 * @desc:
 **/
public class BinaryLogListener {

    public static void main(String[] args) {

        final BinaryLogClient binaryLogClient = new BinaryLogClient(
                "localhost"
                , 3306
                , "root"
                , "zjj20001031"
        );

        binaryLogClient.registerEventListener(event -> {

            System.out.println("new Timestamp(System.currentTimeMillis()) = " + new Timestamp(System.currentTimeMillis()));

            final EventHeader header = event.getHeader();
            System.out.println("event.getHeader().getEventType() = " + header.getEventType());
            System.out.println("header.getDataLength() = " + header.getDataLength());
            System.out.println("header.getServerId() = " + header.getServerId());
            System.out.println("header.getTimestamp() = " + header.getTimestamp());
            System.out.println("event.getData() = " + event.getData());


            System.out.println();
            System.out.println();
            System.out.println();

        });

        try {
            binaryLogClient.connect();
        }catch (Exception e){
            e.printStackTrace();
        }


    }


}
