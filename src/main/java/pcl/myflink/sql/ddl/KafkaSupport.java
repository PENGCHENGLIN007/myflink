package pcl.myflink.sql.ddl;

import java.util.Arrays;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSupport {
	protected static final Logger logger = LoggerFactory.getLogger(KafkaSupport.class);

	public static void main(String[] args) throws Exception {
		
		
		String tablename = "orders";
		String filename = "localfile.csv";
		
		// TODO Auto-generated method stub
/*		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);*/
		//blink environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSettings);
		
		//tableEnv.registerCatalog("myCatalog",new CustomCatalog());
		//tableEnv.registerCatalog("hiveCatalog", new HiveCatalog());
/*		String name = "myhive";
		String defaultDatabase = "default";
		String hiveConfDir = "E:///pengchenglin/Hive/Hive2.3.4/apache-hive-2.3.4-bin/apache-hive-2.3.4-bin/conf";
		String version = "2.3.4";

		HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase,
		hiveConfDir, version);
		tableEnv.registerCatalog(name, hiveCatalog);
		tableEnv.useCatalog(name);*/
		//列出可用的目录、数据库和表
		String[] catalogs = tableEnv.listCatalogs();
		String[] databases = tableEnv.listDatabases();
		String[] tables = tableEnv.listTables();
		logger.info("catalogs="+Arrays.toString(catalogs));
		logger.info("databases="+Arrays.toString(databases));
		logger.info("tables="+Arrays.toString(tables));


		// SQL query with a registered table
		// register a table named "Orders"
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
                + " 'connector.path' = 'C:///Users/Administrator/Desktop/KafkaTest/DDL/"+filename+"', "
                //+ " 'connector.path' = '/home/wangdongxu/"+filename+"', "
                +"   'format.type' = 'csv', "
				+"   'format.fields.0.name' = 'userid', "
				+"   'format.fields.0.type' = 'BIGINT', "
				+"   'format.fields.1.name' = 'product', "
				+"   'format.fields.1.type' = 'VARCHAR', "
				+"   'format.fields.2.name' = 'amount', "
				+"   'format.fields.2.type' = 'INT' "
                + ")");  //-- required: path to a file or directory)");
		// run a SQL query on the Table and retrieve the result as a new Table
		tableEnv.sqlUpdate("CREATE TABLE Orders1 ("
				+ " userid BIGINT,"
				+ " product VARCHAR(200),"
				+ " amount INT) "
				+ "WITH ("
				+"   'connector.type' = 'kafka',        "
				+"   'connector.version' = '0.10', "// "0.8", "0.9", "0.10", "0.11", and "universal
				+"   'connector.property-version' = '1',"
				+"   'connector.topic' = 'topic-pcl-ddl', "
				+"   'update-mode' = 'append', "
				+"   'connector.properties.0.key' = 'zookeeper.connect', "
				+"   'connector.properties.0.value' = '172.16.12.127:2181', "
				+"   'connector.properties.1.key' = 'bootstrap.servers', "
				+"   'connector.properties.1.value' = '172.16.12.127:9092', "
				+"   'connector.properties.2.key' = 'group.id', "
				+"   'connector.properties.2.value' = 'group-pcl-ddl', "
				+"   'connector.startup-mode' = 'earliest-offset',  "
				//+"   'kafka.end-offset='none',  " 
				
				+"   'format.type' = 'csv', "
				+"   'format.fields.0.name' = 'userid', "
				+"   'format.fields.0.type' = 'BIGINT', "
				+"   'format.fields.1.name' = 'product', "
				+"   'format.fields.1.type' = 'VARCHAR', "
				+"   'format.fields.2.name' = 'amount', "
				+"   'format.fields.2.type' = 'INT' "
                + ")");
		
		//tableEnv.sqlUpdate("create view myview  as select * from Orders1");
		
		String[] aftercatalogs = tableEnv.listCatalogs();
		String[] afterdatabases = tableEnv.listDatabases();
		String[] aftertables = tableEnv.listTables();
		logger.info("aftercatalogs="+Arrays.toString(aftercatalogs));
		logger.info("afterdatabases="+Arrays.toString(afterdatabases));
		logger.info("aftertables="+Arrays.toString(aftertables));
		
		String[] aaftertables = tableEnv.listTables();
		logger.info("aaftertables="+Arrays.toString(aaftertables));
		
		Table result = tableEnv.sqlQuery(
		  "SELECT product, amount FROM Orders1 WHERE product LIKE '%glasses%'");
		DataStream<Row> appendStream2 =tableEnv.toAppendStream(result, Row.class);
		appendStream2.print();
		env.execute("DDL01"); 

		// SQL update with a registered table
		// register a TableSink
		//tableEnv.sqlUpdate("CREATE TABLE RubberOrders(product VARCHAR, amount INT) WITH ()");
		// run a SQL update query on the Table and emit the result to the TableSink
		//tableEnv.sqlUpdate(
		 // "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
	}

}
