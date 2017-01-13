package net.hepek.tabulator.api.storage;

import net.hepek.tabulator.api.pojo.DataSourceInfo;
import net.hepek.tabulator.api.pojo.DirectoryInfo;
import net.hepek.tabulator.api.pojo.DirectoryWithSchema;
import net.hepek.tabulator.api.pojo.FileWithSchema;
import net.hepek.tabulator.api.pojo.SchemaInfo;

public interface Storage {

	void save(SchemaInfo schemaInfo);
	void save(FileWithSchema file);
	void save(DataSourceInfo ds);
	void save(DirectoryInfo di);
	void save(DirectoryWithSchema dws);
	void close();
	long getLastModified(String path);
	void saveLastModified(String path, long modificationTime);
	void cleanCaches();
	
}