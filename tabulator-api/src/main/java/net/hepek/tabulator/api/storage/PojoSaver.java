package net.hepek.tabulator.api.storage;

import net.hepek.tabulator.api.pojo.DataSourceInfo;
import net.hepek.tabulator.api.pojo.FileWithSchema;
import net.hepek.tabulator.api.pojo.SchemaInfo;

public interface PojoSaver {

	void save(SchemaInfo schemaInfo);
	void save(FileWithSchema file);
	void save(DataSourceInfo ds);
	void close();
	
}