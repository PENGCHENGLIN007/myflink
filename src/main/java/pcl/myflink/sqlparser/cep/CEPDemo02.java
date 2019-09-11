package pcl.myflink.sqlparser.cep;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

public class CEPDemo02 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
		tableEnv.connect(
				new Kafka()
						.version("0.10")
						// "0.8", "0.9", "0.10", "0.11", and "universal"
						.topic("jsontest")
						.property("bootstrap.servers", "mt-mdh.local:9093")
						.property("group.id", "test").startFromLatest())
				.withFormat(new Json().failOnMissingField(false).deriveSchema())
				.withSchema(
						new Schema()
								.field("rowtime", Types.SQL_TIMESTAMP)
								.rowtime(
										new Rowtime()
												.timestampsFromField(
														"eventtime")
												.watermarksPeriodicBounded(2000))
								.field("fruit", Types.STRING)
								.field("number", Types.INT)).inAppendMode()
				.registerTableSource("source");
		
		tableEnv.connect(
				new Kafka()
						.version("0.10")
						// "0.8", "0.9", "0.10", "0.11", and "universal"
						.topic("test").property("acks", "all")
						.property("retries", "0")
						.property("batch.size", "16384")
						.property("linger.ms", "10")
						.property("bootstrap.servers", "mt-mdh.local:9093")
						.sinkPartitionerFixed())
				.inAppendMode()
				.withFormat(new Json().deriveSchema())
				.withSchema(
						new Schema().field("fruit", Types.STRING)
								.field("total", Types.INT)
								.field("time", Types.SQL_TIMESTAMP))
				.registerTableSink("sink");
		tableEnv.sqlUpdate("insert into sink"
				+ " select fruit,sum(number),TUMBLE_END(rowtime, INTERVAL '5' SECOND) "
				+ "from source group by fruit,TUMBLE(rowtime, INTERVAL '5' SECOND)");
		env.execute();
	}
}
