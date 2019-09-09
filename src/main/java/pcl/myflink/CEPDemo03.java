package pcl.myflink;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建topic ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic-cep-test03
* 
* @Description: TODO 
* @author : pengchenglin
* @date : Aug 14, 2019 2:58:21 PM 
* @version V1.0
 */
public class CEPDemo03 {
	protected static final Logger logger = LoggerFactory.getLogger("CEPDemo");

	public static void main(String[] arg) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tableEnv.connect(new Kafka()
                    .version("0.10")
                    .topic("topic-cep-test03")
                    //.property("group.id", "testGroup")
                    //.property("zookeeper.connect","")
                    //.startFromLatest()    
                    //.startFromSpecificOffsets(...)
                    .property("bootstrap.servers","172.16.12.127:9092")
                    .startFromEarliest()
                        // optional: output partitioning from Flink's partitions into Kafka's partitions    
                    //.sinkPartitionerFixed() // each Flink partition ends up in at-most one Kafka partition (default)    
                    //.sinkPartitionerRoundRobin()    // a Flink partition is distributed to Kafka partitions round-robin    
                    //.sinkPartitionerCustom(MyCustom.class)    // use a custom FlinkKafkaPartitioner subclass)
                    .sinkPartitionerRoundRobin()//Flink分区随机映射到kafka分区
//Specifies the format that defines how to read data from a connector.
            ).withFormat(new Json()
                    .failOnMissingField(false)
                    //If set to true, the operation fails if there is a missing field.
	                //If set to false, a missing field is set to null.
                    .deriveSchema()

            ).withSchema(new Schema()
            //Adds a field with the field name and the type string. Required.
            //This method can be called multiple times. 
            //The call order of this method defines also the order of the fields in a row.
            //Field names are matched by the exact name by default (case sensitive).
                    .field("symbol", Types.STRING).from("symbol")
                    .field("price", Types.INT).from("price")
                    .field("tax", Types.INT).from("tax")
                    .field("rowtime", Types.SQL_TIMESTAMP)
                    .rowtime(new Rowtime()
                            .timestampsFromField("eventtime")
                            .watermarksPeriodicBounded(500)
                    )
                   // .field("proctime", Types.SQL_TIMESTAMP).proctime()
            ).inAppendMode().registerTableSource("Ticker");

        Table tb2 = tableEnv.sqlQuery(
        		" SELECT * "+
        				" FROM Ticker "+
        				" MATCH_RECOGNIZE ( "+
        				"     PARTITION BY symbol "+
        				"     ORDER BY rowtime "+
        				"     MEASURES "+
        				"         START_ROW.rowtime AS start_tstamp, "+
        				"         LAST(PRICE_DOWN.rowtime) AS bottom_tstamp, "+
        				"         LAST(PRICE_UP.rowtime) AS end_tstamp "+
        				"     ONE ROW PER MATCH "+
        				"     AFTER MATCH SKIP TO LAST PRICE_UP "+
        				"     PATTERN (START_ROW PRICE_DOWN+ PRICE_UP) "+
        				"     DEFINE "+
        				"         PRICE_DOWN AS "+
        				"             (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR "+
        				"                 PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1), "+
        				"         PRICE_UP AS "+
        				"             PRICE_UP.price > LAST(PRICE_DOWN.price, 1) "+
        				"     ) MR "+
        				//" GROUP BY TUMBLE(MR.rowtime, INTERVAL '10' SECOND) "
        				""
                );

           DataStream<Row> appendStream =tableEnv.toAppendStream(tb2, Row.class);

            System.out.println("schema is:");
            tb2.printSchema();

        appendStream.writeAsText("/usr/local/02", WriteMode.OVERWRITE);

        logger.info("stream end");  

        Table tb3 = tableEnv.sqlQuery("select  *  from Ticker");
        DataStream<Row> temp =tableEnv.toAppendStream(tb3, Row.class);
        tb3.printSchema();
        temp.writeAsText("/usr/local/022", WriteMode.OVERWRITE);

        env.execute("msg test");    

    }

}
