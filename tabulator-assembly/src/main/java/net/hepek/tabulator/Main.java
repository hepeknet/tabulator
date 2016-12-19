package net.hepek.tabulator;

import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import net.hepek.tabulator.api.ds.DataSourceProcessor;
import net.hepek.tabulator.api.storage.PojoSaver;
import net.hepek.tabulator.storage.StorageLoader;

public class Main {
	
	private static final int DEFAULT_INITIAL_DELAY_SECS = 5;

	private static Logger LOG = LoggerFactory.getLogger(Main.class);

	private static ServiceLoader<DataSourceProcessor> processorLoader = null;

	public static void main(String[] args) {
		LOG.debug("Starting...");
		List<DataSourceConfiguration> configuredDataSources = Util.getGloballyConfiguredDataSources();
		if (configuredDataSources == null || configuredDataSources.isEmpty()) {
			throw new IllegalStateException("Did not find any configured data sources. Nothing for me to do here!");
		}
		processorLoader = ServiceLoader.load(DataSourceProcessor.class);
		if (processorLoader == null) {
			throw new IllegalStateException("Was not able to find any data processors...");
		}
		for (DataSourceConfiguration dsc : configuredDataSources) {
			ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
			String uri = dsc.getUri();
			DataSourceProcessor dsp = findAppropriateProcessor(uri);
			exec.scheduleAtFixedRate(() -> {
				LOG.debug("Submitted {} for processing", uri);
				PojoSaver storage = loadStorage();
				dsp.processDataSource(uri, storage);
				storage.close();
			}, DEFAULT_INITIAL_DELAY_SECS, dsc.getRefreshTimeSeconds(), TimeUnit.SECONDS);
			LOG.debug("Datasource {} is scheduled to be refreshed every {} seconds", uri, dsc.getRefreshTimeSeconds());
		}
		Config conf = ConfigFactory.load();
		List<String> exts = conf.getStringList("tabulator.parquet.file-extensions");
		System.out.println(exts);
	}

	private static PojoSaver loadStorage() {
		return StorageLoader.loadStorage();
	}

	private static DataSourceProcessor findAppropriateProcessor(String uri) {
		Iterator<DataSourceProcessor> processors = processorLoader.iterator();
		DataSourceProcessor found = null;
		while (processors.hasNext()) {
			DataSourceProcessor p = processors.next();
			boolean fits = p.understandsDataSourceUri(uri);
			if (fits) {
				if (found != null) {
					throw new IllegalStateException("Found more than one processor that understands uri [" + uri
							+ "]. Probably classpath issue!");
				}
				found = p;
			}
		}
		if (found == null) {
			throw new IllegalStateException(
					"Was not able to find processor for URI [" + uri + "]. Probably classpath issue!");
		}
		LOG.debug("Found exactly one processor {} for uri {}", found, uri);
		return found;
	}

}
