package pcl.myflink.sqlparser.ddl;

import java.util.Arrays;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveSupport {
	protected static final Logger logger = LoggerFactory.getLogger(HiveSupport.class);

	public static void main(String[] args) throws Exception {
		
		String tablename = args[0];
		String filename = args[1];
		
		// TODO Auto-generated method stub
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
		
		String name = "myhive";
		String defaultDatabase = "pcl_flilnk_ddl";
		//String hiveConfDir = "/home/hadoop/cpb/hive-3.1.1/conf";//31
		String hiveConfDir = "/home/hadoop/cpb/hive-2.3.3/conf";//32
		String version = "2.3.4";
		
		String[] catalogs1 = tableEnv.listCatalogs();
		String[] databases1 = tableEnv.listDatabases();
		String[] tables1 = tableEnv.listTables();
		logger.info("catalogs1="+Arrays.toString(catalogs1));
		logger.info("databases1="+Arrays.toString(databases1));
		logger.info("tables1="+Arrays.toString(tables1));

		HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase,
		hiveConfDir, version);
		tableEnv.registerCatalog(name, hiveCatalog);
		tableEnv.useCatalog(name);
		
		String[] catalogs2 = tableEnv.listCatalogs();
		String[] databases2 = tableEnv.listDatabases();
		String[] tables2 = tableEnv.listTables();
		logger.info("catalogs2="+Arrays.toString(catalogs2));
		logger.info("databases2="+Arrays.toString(databases2));
		logger.info("tables2="+Arrays.toString(tables2));
		
		tableEnv.sqlUpdate("CREATE TABLE "+tablename+" ("
				+ " userid BIGINT,"
				+ " product VARCHAR(200),"
				+ " amount INT "
				//+ " primary key (userid)"
				//+ " proctime AS PROCTIME(),"
				//+ " WATERMARK FOR order_ts AS BOUNDED WITH DELAY '10' SECOND "
				+ " )"
				+ "WITH ("
				+ " 'connector.type' = 'filesystem', "//               -- required: specify to connector type
                //+ " 'connector.path' = 'C:///Users/Administrator/Desktop/KafkaTest/DDL/"+filename+"', "
                + " 'connector.path' = '/home/pengchenglin/"+filename+"', "
                +"   'format.type' = 'csv', "
				+"   'format.fields.0.name' = 'userid', "
				+"   'format.fields.0.type' = 'BIGINT', "
				+"   'format.fields.1.name' = 'product', "
				+"   'format.fields.1.type' = 'VARCHAR', "
				+"   'format.fields.2.name' = 'amount', "
				+"   'format.fields.2.type' = 'INT' "
                + ")");
		
		
		String[] catalogs3 = tableEnv.listCatalogs();
		String[] databases3 = tableEnv.listDatabases();
		String[] tables3 = tableEnv.listTables();
		logger.info("catalogs3="+Arrays.toString(catalogs3));
		logger.info("databases3="+Arrays.toString(databases3));
		logger.info("tables3="+Arrays.toString(tables3));
		
		
		Table result = tableEnv.sqlQuery(
				  "SELECT product, amount FROM "+tablename+" WHERE product LIKE '%glasses%'");
				DataStream<Row> appendStream2 =tableEnv.toAppendStream(result, Row.class);
				appendStream2.print().name("HiveSupport");
				env.execute("HiveSupport");
	}

}
