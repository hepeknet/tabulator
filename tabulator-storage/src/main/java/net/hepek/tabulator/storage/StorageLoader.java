package net.hepek.tabulator.storage;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.hepek.tabulator.Util;
import net.hepek.tabulator.api.storage.Storage;
import net.hepek.tabulator.storage.es.BulkElasticSearchStorage;

public class StorageLoader {
	
	private static Logger LOG = LoggerFactory.getLogger(StorageLoader.class);

	public static Storage loadStorage() {
		final List<String> clusterNodes = Util.getStorageConfiguration();
		LOG.debug("Loading storage for {}", clusterNodes);
		//return new ElasticSearchStorage(clusterNodes);
		return new BulkElasticSearchStorage(clusterNodes);
	}
	
}
