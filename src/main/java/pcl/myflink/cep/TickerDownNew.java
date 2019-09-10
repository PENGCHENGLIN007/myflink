package pcl.myflink.cep;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TickerDownNew {

		protected static final Logger logger = LoggerFactory.getLogger(TickerDownNew.class);

		public static void main(String[] arg) throws Exception {

	        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

	        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

	        tableEnv.connect(new Kafka()
	                    .version("0.10")
	                    .topic("cep-ChinaUnicom")
	                    .property("group.id", "ChinaUnicom-cep-test01")
	                    //.property("zookeeper.connect","")
	                    //.startFromLatest()    
	                    //.startFromSpecificOffsets(...)
	                    .startFromEarliest()
	                    .property("bootstrap.servers","172.16.12.127:9092")
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
	                    //.field("eventtime", Types.STRING).from("eventtime")
	                    .field("rowtime", Types.SQL_TIMESTAMP)
	                    .rowtime(new Rowtime()
	                    //*设置内置时间戳提取程序，该提取程序可转换现有的[[Long]]或
	                    //*将[[Types.SQL_TIMESTAMP]]字段输入rowtime属性。
	                            .timestampsFromField("eventtime")
	                            .watermarksPeriodicBounded(100)
	                    )
	                    //.field("proctime", Types.SQL_TIMESTAMP).proctime()
	            ).inAppendMode().registerTableSource("Ticker");
	        Table tb1 =tableEnv.sqlQuery("select * from Ticker");
	        tb1.printSchema();
	        DataStream<Row> appendStream1 =tableEnv.toAppendStream(tb1, Row.class);
	        appendStream1.print();
	        Table tb2 = tableEnv.sqlQuery(
	        		" SELECT * "+
	        				" FROM Ticker "+
	        				" MATCH_RECOGNIZE ( "+
	        				"     PARTITION BY symbol "+
	        				"     ORDER BY rowtime "+
	        				"     MEASURES "+
	        				"         START_ROW.eventtime AS start_tstamp, "+
	        				"         LAST(PRICE_DOWN.eventtime) AS bottom_tstamp, "+
	        				"         LAST(PRICE_UP.eventtime) AS end_tstamp "+
	        				"     ONE ROW PER MATCH "+
	        				"     AFTER MATCH SKIP TO LAST PRICE_UP "+
	        				"     PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)  "+
	        				"     WITHIN INTERVAL '1' minute  "+
	        				"     DEFINE "+
	        				"         PRICE_DOWN AS "+
	        				"             (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR "+
	        				"                 PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1), "+
	        				"         PRICE_UP AS "+
	        				"			PRICE_UP.price > LAST(PRICE_DOWN.price, 1) "+
	        				"     ) MR "+
	        				""
	                );

	           DataStream<Row> appendStream =tableEnv.toAppendStream(tb2, Row.class);

	            System.out.println("schema is:");
	            tb2.printSchema();
	            appendStream.print().name("ChinaUnicom");


	        env.execute("ChinaUnicom");    

	    }


}
