package net.hepek.tabulator;

import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.hepek.tabulator.api.ds.DataSource;
import net.hepek.tabulator.api.ds.DataSourceProcessor;
import net.hepek.tabulator.api.storage.Storage;
import net.hepek.tabulator.storage.StorageLoader;

public class Main {

	private static final int MAIN_THREAD_SLEEP_TIME = 30 * 1000;

	private static final int DEFAULT_INITIAL_DELAY_SECS = 10;

	private static Logger LOG = LoggerFactory.getLogger(Main.class);

	private static ServiceLoader<DataSourceProcessor> processorLoader = null;

	public static void main(String[] args) throws Exception {
		LOG.debug("Starting...");
		final List<DataSourceConfiguration> configuredDataSources = Util.getGloballyConfiguredDataSources();
		if (configuredDataSources == null || configuredDataSources.isEmpty()) {
			throw new IllegalStateException("Did not find any configured data sources. Nothing for me to do here!");
		}
		processorLoader = ServiceLoader.load(DataSourceProcessor.class);
		if (processorLoader == null) {
			throw new IllegalStateException("Was not able to find any data processors...");
		}
		final boolean shouldCleanCache = Util.shouldCleanCache();
		LOG.debug("Should clean cache {}", shouldCleanCache);
		if (shouldCleanCache) {
			Thread.sleep(DEFAULT_INITIAL_DELAY_SECS * 1000);
			final Storage storage = loadStorage();
			storage.cleanCaches();
			storage.close();
			LOG.debug("Cleaned cache");
		}
		for (final DataSourceConfiguration dsc : configuredDataSources) {
			final ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
			final String uri = dsc.getUri();
			final DataSource ds = new DataSource();
			ds.setUri(uri);
			ds.setTags(dsc.getTags());
			ds.setProperties(dsc.getProperties());
			final DataSourceProcessor dsp = findAppropriateProcessor(ds);
			exec.scheduleWithFixedDelay(() -> {
				try {
					LOG.debug("Submitted {} for processing", uri);
					final Storage storage = loadStorage();
					LOG.debug("Loaded storage {}", storage);

					dsp.processDataSource(ds, storage);
					LOG.debug("Processed {}", uri);
					storage.close();
				} catch (final Throwable t) {
					LOG.error("Error while processing {}", uri, t);
				}
			}, DEFAULT_INITIAL_DELAY_SECS, dsc.getRefreshTimeSeconds(), TimeUnit.SECONDS);

			LOG.debug("Datasource {} is scheduled to be refreshed every {} seconds", uri, dsc.getRefreshTimeSeconds());
		}
		while (true) {
			Thread.sleep(MAIN_THREAD_SLEEP_TIME);
			LOG.debug(".");
		}
	}

	private static Storage loadStorage() {
		LOG.debug("Loading storage...");
		return StorageLoader.loadStorage();
	}

	private static DataSourceProcessor findAppropriateProcessor(DataSource ds) {
		final Iterator<DataSourceProcessor> processors = processorLoader.iterator();
		DataSourceProcessor found = null;
		while (processors.hasNext()) {
			final DataSourceProcessor p = processors.next();
			final boolean fits = p.understandsDataSourceUri(ds);
			if (fits) {
				if (found != null) {
					throw new IllegalStateException("Found more than one processor that understands uri [" + ds.getUri()
							+ "]. Probably classpath issue!");
				}
				found = p;
			}
		}
		if (found == null) {
			throw new IllegalStateException(
					"Was not able to find processor for URI [" + ds.getUri() + "]. Probably classpath issue!");
		}
		LOG.debug("Found exactly one processor {} for uri {}", found, ds.getUri());
		return found;
	}

}
