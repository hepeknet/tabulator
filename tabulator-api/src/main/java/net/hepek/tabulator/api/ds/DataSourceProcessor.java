package net.hepek.tabulator.api.ds;

import net.hepek.tabulator.api.storage.Storage;

public interface DataSourceProcessor {

	boolean understandsDataSourceUri(DataSource ds);
	
	void processDataSource(DataSource ds, Storage saver);
	
}
