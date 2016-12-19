package net.hepek.tabulator.storage;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.hepek.tabulator.Util;
import net.hepek.tabulator.api.storage.PojoSaver;
import net.hepek.tabulator.storage.es.ElasticSearchSaver;

public class StorageLoader {
	
	private static Logger LOG = LoggerFactory.getLogger(StorageLoader.class);

	public static PojoSaver loadStorage() {
		List<String> clusterNodes = Util.getStorageConfiguration();
		LOG.debug("Loading storage for {}", clusterNodes);
		return new ElasticSearchSaver(clusterNodes);
	}
	
}
