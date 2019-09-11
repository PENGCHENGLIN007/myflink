package pcl.myflink.sqlparser.cep;

import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ChinaUnicomFromKafkaSinkKafka {

		protected static final Logger logger = LoggerFactory.getLogger(ChinaUnicomFromKafkaSinkKafka.class);

		public static void main(String[] arg) throws Exception {
			String topic  ="pcl-ChinaUnicom-kafka";
			if(arg.length==1){
				topic = arg[0];
				
			}

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
			StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
	        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	        
	        Properties properties = new Properties();
			properties.setProperty("bootstrap.servers", "172.16.12.127:9092");
			properties.setProperty("group.id", "pcl01");
			FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>(topic,
					new SimpleStringSchema(Charset.forName("utf8")),properties);
			//consumer.setStartFromEarliest();//从最早记录开始

			DataStream<String> stream = env.addSource(consumer);
			//if(isPrintStream)
			//stream.print();
			@SuppressWarnings("rawtypes")
			TypeInformation[] types ={Types.STRING,Types.INT,Types.INT,Types.LONG};
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
							row.setField(i,Integer.valueOf(str[i]));
							break;
						}
						case 3:{
							Timestamp tms = Timestamp.valueOf(str[i]);
							row.setField(i,tms.getTime());
							break;
						}
						}
					}
					return row;
				}
		        }).returns(new RowTypeInfo(types) );
			DataStream<Row> ithTimestampsAndWatermarks = streamaa
	                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>(){

						private static final long serialVersionUID = 1L;

						private final long maxOutOfOrderness = 0; // 3.5 seconds

	                    private long currentMaxTimestamp;

	                    @Override
	                    public Watermark getCurrentWatermark() {
	                        // TODO Auto-generated method stub
	                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	                    }

	                	@Override
	                	public long extractTimestamp(Row element, long previousElementTimestamp) {
	                		// TODO Auto-generated method stub
	                		long timestamp= 0;
	                		timestamp = (long) element.getField(3);
	                		//timestamp = (Long)element.getField(1);
	                		currentMaxTimestamp = timestamp;
	                		return timestamp;
	                	}
	                	
	                });
				tableEnv.registerDataStream("fence",ithTimestampsAndWatermarks,
						"phone,lac,cell,eventtime,rowtime.rowtime");
				/*Table tb1 = tableEnv.sqlQuery("select * from fence ");
				DataStream<Row> appendStream =tableEnv.toAppendStream(tb1, Row.class);
				tb1.printSchema();
		           appendStream.print().name("TickerDown");*/
		           
				Table tb2 = tableEnv.sqlQuery(
						" SELECT "+
		        				" * "+
		        				" FROM fence "+
		        				" MATCH_RECOGNIZE ( "+
		        				"     PARTITION BY phone "+
		        				"     ORDER BY rowtime "+
		        				"     MEASURES "+
		        				"          FIRST(USER_IN.eventtime) AS start_in_time "+
		        				"        , LAST(USER_IN.eventtime) AS end_in_time"+
		        				"		 , LAST(USER_IN.eventtime)-FIRST(USER_IN.eventtime) as stay_time "+
		        				"     ONE ROW PER MATCH "+
		        				"     AFTER MATCH SKIP PAST LAST ROW "+
		        				"     PATTERN (USER_IN +? USER_STADY  )  "+
		        				"     WITHIN INTERVAL '5' MINUTE  "+
		        				"     DEFINE "+
		        				"		  USER_STADY AS "+
		        				"			 LAST(USER_IN.eventtime)"+
		        				"			 -FIRST(USER_IN.eventtime)>60000,"+
		        				"         USER_IN AS "+
		        				"             USER_IN.lac = 123 "+
		        				" 			  AND USER_IN.cell = 456 "+
		        				"     ) MR "+
		        				""
		                );

		           DataStream<Row> appendStream2 =tableEnv.toAppendStream(tb2, Row.class);
					DataStream<String> streamaa2 = appendStream2.map(new MapFunction<Row, String>() {

						private static final long serialVersionUID = 1L;

						@Override
						public String map(Row value) throws Exception {
							return value.toString();
						}
				        });
		           
					streamaa2.addSink(new FlinkKafkaProducer010<>(
		   				"172.16.12.127:9092", 
		   				"sink-ChinaUnicom", 
		   				new SimpleStringSchema()))
		   				.setParallelism(1);
	        env.execute("ChinaUnicom");    

	    }
}
