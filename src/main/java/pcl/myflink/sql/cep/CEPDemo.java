package pcl.myflink.sql.cep;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建topic ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic-cep-test01
* 
* @Description: TODO 
* @author : pengchenglin
* @date : Aug 14, 2019 2:58:21 PM 
* @version V1.0
 */
public class CEPDemo {
	protected static final Logger logger = LoggerFactory.getLogger("CEPDemo");

	public static void main(String[] arg) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tableEnv.connect(new Kafka()
                    .version("0.10")
                    .topic("topic-cep-test01")
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
                    .field("sessionId", Types.STRING).from("sessionId")
            //Field names are matched by the exact name by default (case sensitive).
                    .field("fromUid", Types.STRING).from("fromUid")
                    .field("toUid", Types.STRING).from("toUid")
                    .field("chatType", Types.STRING).from("chatType")
                    .field("type", Types.STRING).from("type")
                    .field("msgId", Types.STRING).from("msgId")
                    .field("msg", Types.STRING).from("msg")
                  //.field("timestampSend", Types.SQL_TIMESTAMP)
                    .field("rowtime", Types.SQL_TIMESTAMP)
                    .rowtime(new Rowtime()
                    //*设置内置时间戳提取程序，该提取程序可转换现有的[[Long]]或
                    //*将[[Types.SQL_TIMESTAMP]]字段输入rowtime属性。
                            .timestampsFromField("timestampSend")
                            .watermarksPeriodicBounded(1000)
                    )
                    //.field("proctime", Types.SQL_TIMESTAMP).proctime()
            ).inAppendMode().registerTableSource("myTable");

        Table tb2 = tableEnv.sqlQuery(
                "SELECT " +
                        " answerTime, customer_event_time, empUid, noreply_counts, total_talk " +
                        " FROM myTable" +
                        " MATCH_RECOGNIZE ( " +
                        " PARTITION BY sessionId " +
                        " ORDER BY rowtime " +
                        " MEASURES " +
                        " e2.rowtime as answerTime, "+
                        " LAST(e1.rowtime) as customer_event_time, " +
                        " e2.fromUid as empUid, " +
                        " 1 as noreply_counts, " +
                        " e1.rowtime as askTime," +                      
                        " 1 as total_talk " +          
                        " ONE ROW PER MATCH " +
                        " AFTER MATCH SKIP TO LAST e2 " +
                        " PATTERN (e1 e2) " +
                        " DEFINE " +
                        " e1 as e1.type = 'yonghu', " +
                        " e2 as e2.type = 'guanjia' " +
                        " ) "
                        //+ "GROUP BY TUMBLE(timestampSend, INTERVAL '10' SECOND)"
                );

           DataStream<Row> appendStream =tableEnv.toAppendStream(tb2, Row.class);

            System.out.println("schema is:");
            tb2.printSchema();

        appendStream.writeAsText("/usr/local/127whk", WriteMode.OVERWRITE);

        logger.info("stream end");  

        Table tb3 = tableEnv.sqlQuery("select  sessionId, type  from myTable");
        DataStream<Row> temp =tableEnv.toAppendStream(tb3, Row.class);
        tb3.printSchema();
        temp.writeAsText("/usr/local/127whk2", WriteMode.OVERWRITE);

        env.execute("msg test");    

    }

}
