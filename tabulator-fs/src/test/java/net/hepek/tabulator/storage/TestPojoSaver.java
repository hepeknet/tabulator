package net.hepek.tabulator.storage;

import net.hepek.tabulator.api.pojo.DataSourceInfo;
import net.hepek.tabulator.api.pojo.FileWithSchema;
import net.hepek.tabulator.api.pojo.SchemaInfo;
import net.hepek.tabulator.api.storage.PojoSaver;

public class TestPojoSaver implements PojoSaver {
	
	private SchemaInfo si;
	private FileWithSchema fws;
	private DataSourceInfo dsi;
	

	public void save(SchemaInfo schemaInfo) {
		si = schemaInfo;
	}

	public void save(FileWithSchema file) {
		fws = file;
	}

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

}
