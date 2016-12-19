package net.hepek.tabulator.api.ds;

import net.hepek.tabulator.api.storage.PojoSaver;

public interface DataSourceProcessor {

	boolean understandsDataSourceUri(String uri);
	
	void processDataSource(String uri, PojoSaver saver);
	
}
