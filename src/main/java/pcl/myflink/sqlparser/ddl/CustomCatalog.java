package pcl.myflink.sqlparser.ddl;

import java.util.List;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;

public class CustomCatalog implements Catalog{


	@Override
	public void alterDatabase(String arg0, CatalogDatabase arg1, boolean arg2)
			throws DatabaseNotExistException, CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alterFunction(ObjectPath arg0, CatalogFunction arg1,
			boolean arg2) throws FunctionNotExistException, CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alterPartition(ObjectPath arg0, CatalogPartitionSpec arg1,
			CatalogPartition arg2, boolean arg3)
			throws PartitionNotExistException, CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alterPartitionColumnStatistics(ObjectPath arg0,
			CatalogPartitionSpec arg1, CatalogColumnStatistics arg2,
			boolean arg3) throws PartitionNotExistException, CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alterPartitionStatistics(ObjectPath arg0,
			CatalogPartitionSpec arg1, CatalogTableStatistics arg2, boolean arg3)
			throws PartitionNotExistException, CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alterTable(ObjectPath arg0, CatalogBaseTable arg1, boolean arg2)
			throws TableNotExistException, CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alterTableColumnStatistics(ObjectPath arg0,
			CatalogColumnStatistics arg1, boolean arg2)
			throws TableNotExistException, CatalogException,
			TablePartitionedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alterTableStatistics(ObjectPath arg0,
			CatalogTableStatistics arg1, boolean arg2)
			throws TableNotExistException, CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createDatabase(String arg0, CatalogDatabase arg1, boolean arg2)
			throws DatabaseAlreadyExistException, CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createFunction(ObjectPath arg0, CatalogFunction arg1,
			boolean arg2) throws FunctionAlreadyExistException,
			DatabaseNotExistException, CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createPartition(ObjectPath arg0, CatalogPartitionSpec arg1,
			CatalogPartition arg2, boolean arg3) throws TableNotExistException,
			TableNotPartitionedException, PartitionSpecInvalidException,
			PartitionAlreadyExistsException, CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createTable(ObjectPath arg0, CatalogBaseTable arg1, boolean arg2)
			throws TableAlreadyExistException, DatabaseNotExistException,
			CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean databaseExists(String arg0) throws CatalogException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void dropDatabase(String arg0, boolean arg1)
			throws DatabaseNotExistException, DatabaseNotEmptyException,
			CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dropFunction(ObjectPath arg0, boolean arg1)
			throws FunctionNotExistException, CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dropPartition(ObjectPath arg0, CatalogPartitionSpec arg1,
			boolean arg2) throws PartitionNotExistException, CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dropTable(ObjectPath arg0, boolean arg1)
			throws TableNotExistException, CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean functionExists(ObjectPath arg0) throws CatalogException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public CatalogDatabase getDatabase(String arg0)
			throws DatabaseNotExistException, CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDefaultDatabase() throws CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CatalogFunction getFunction(ObjectPath arg0)
			throws FunctionNotExistException, CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CatalogPartition getPartition(ObjectPath arg0,
			CatalogPartitionSpec arg1) throws PartitionNotExistException,
			CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(
			ObjectPath arg0, CatalogPartitionSpec arg1)
			throws PartitionNotExistException, CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(ObjectPath arg0,
			CatalogPartitionSpec arg1) throws PartitionNotExistException,
			CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath arg0)
			throws TableNotExistException, CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath arg0)
			throws TableNotExistException, CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath arg0)
			throws TableNotExistException, CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> listDatabases() throws CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> listFunctions(String arg0)
			throws DatabaseNotExistException, CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath arg0)
			throws TableNotExistException, TableNotPartitionedException,
			CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<CatalogPartitionSpec> listPartitions(ObjectPath arg0,
			CatalogPartitionSpec arg1) throws TableNotExistException,
			TableNotPartitionedException, CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> listTables(String arg0)
			throws DatabaseNotExistException, CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> listViews(String arg0)
			throws DatabaseNotExistException, CatalogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void open() throws CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean partitionExists(ObjectPath arg0, CatalogPartitionSpec arg1)
			throws CatalogException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void renameTable(ObjectPath arg0, String arg1, boolean arg2)
			throws TableNotExistException, TableAlreadyExistException,
			CatalogException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean tableExists(ObjectPath arg0) throws CatalogException {
		// TODO Auto-generated method stub
		return false;
	}

}
