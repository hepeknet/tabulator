package net.hepek.tabulator.storage;

import net.hepek.tabulator.api.pojo.DataSourceInfo;
import net.hepek.tabulator.api.pojo.FileWithSchema;
import net.hepek.tabulator.api.pojo.SchemaInfo;
import net.hepek.tabulator.api.storage.Storage;

public class TestPojoSaver implements Storage {
	
	private SchemaInfo si;
	private FileWithSchema fws;
	private DataSourceInfo dsi;
	

	@Override
	public void save(SchemaInfo schemaInfo) {
		si = schemaInfo;
	}

	@Override
	public void save(FileWithSchema file) {
		fws = file;
	}

	@Override
	public void save(DataSourceInfo ds) {
		dsi = ds;
	}

	public SchemaInfo getSchemaInfo() {
		return si;
	}

	public FileWithSchema getFileWithSchema() {
		return fws;
	}

	public DataSourceInfo getDataSourceInfo() {
		return dsi;
	}

	@Override
	public void close() {
		
	}

	@Override
	public long getLastModified(String path) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void saveLastModified(String path, long modificationTime) {
		// TODO Auto-generated method stub
		
	}

}
