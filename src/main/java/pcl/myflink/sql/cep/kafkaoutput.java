package pcl.myflink.sql.cep;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Kafka
 * @author Administrator
 *
 *Flink的Kafka消费者被称为FlinkKafkaConsumer08（或09Kafka 0.9.0.x等）。它提供对一个或多个Kafka主题的访问。
 *
 *1.主题名称/主题名称列表     list
 *2.DeserializationSchema / KeyedDeserializationSchema用于反序列化来自Kafka的数据
 *3.Kafka消费者的属性。需要以下属性：
 *		bootstrap.servers （以逗号分隔的Kafka经纪人名单） 
 *		zookeeper.connect （逗号分隔的Zookeeper服务器列表）（仅Kafka 0.8需要）
 *		group.id 消费者群组的ID
 *
 */
public class kafkaoutput {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "172.16.12.127:9092");
		//properties.setProperty("zookeeper.connect", "172.16.44.28:2180,172.16.44.29:2180,172.16.44.30:2180");
		//properties.setProperty("group.id", "pcl01");
		
		FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("cep-testoutput",
				new SimpleStringSchema(Charset.forName("utf8")),properties);
		consumer.setStartFromEarliest();//从最早记录开始
		//consumer.setStartFromLatest();//从最新记录开始
		DataStream<String> stream = env.addSource(consumer);
		stream.print();
		//TypeInformation[] types ={Types.STRING,Types.LONG,Types.INT,Types.INT};
		@SuppressWarnings("rawtypes")
		TypeInformation[] types ={Types.STRING,Types.INT,Types.INT,Types.SQL_TIMESTAMP};
		DataStream<Row> streamaa = stream.map(new MapFunction<String, Row>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row map(String value) throws Exception {
				// TODO Auto-generated method stub
				String[] str = value.split(",");
				Row row = new Row(4);
				for(int i=0 ;i<str.length;i++){
					switch(i){
					case 0:{
						row.setField(i,(String)str[i]);
						break;
					}

					case 1:
					case 2:{
						row.setField(i,Integer.parseInt(str[i]));
						break;
					}
					case 3:{
						Timestamp tms = Timestamp.valueOf(str[i]);
						row.setField(i,tms);
						break;
					}
					}
				}
				return row;
			}
	        }).returns(new RowTypeInfo(types) );
		DataStream<Row> ithTimestampsAndWatermarks = streamaa
                .assignTimestampsAndWatermarks(new WatermarkOutput());
			tableEnv.registerDataStream("Ticker",ithTimestampsAndWatermarks,
					"symbol,tax,price,eventtime,rowtime.rowtime");
			//tableEnv.registerDataStream("Ticker",streamaa, "symbol,eventtime,price,tax");
			//tableEnv.registerDataStream("Ticker",streamaa, "symbol,eventtime,price,tax");
			
			Table tb2 = tableEnv.sqlQuery(
	        		" SELECT * "+
	        				" FROM Ticker "+
	        				" MATCH_RECOGNIZE ( "+
	        				"     PARTITION BY symbol "+
	        				"     ORDER BY rowtime "+
	        				"     MEASURES "+
	        				"         FIRST(A.price) AS startPrice, "+
	        				"         LAST(A.price) AS topPrice, "+
	        				"         B.price AS lastPric "+
	        				"     ONE ROW PER MATCH "+
	        				"     PATTERN (START_ROW A+ B)  "+ 
	        				"     WITHIN INTERVAL '1' minute  "+
	        				"     DEFINE "+
	        				"         A AS "+
	        				"				A.price < 2, "+
	        				///"             LAST(A.price, 1) IS NULL OR A.price > LAST(A.price, 1), "+
	        				"		  B AS "+
	        				////"             B.price < LAST(A.price) "+
	        				"             B.price > 1 "+
	        				"     ) MR "+
	        				""
	                );

	           DataStream<Row> appendStream =tableEnv.toAppendStream(tb2, Row.class);
	           System.out.println("result:");
	           appendStream.print();

	            System.out.println("schema is:");
	            tb2.printSchema();

	        //appendStream.writeAsText("/usr/local/outputcep", WriteMode.OVERWRITE);

			
			
		env.execute();
		
	}
}
