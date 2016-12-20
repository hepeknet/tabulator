package net.hepek.tabulator;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;

public abstract class Util {

	private static final Logger logger = LoggerFactory.getLogger(Util.class);

	private static Config GLOBAL_CONFIG;

	public static Config getInternalConfig() {
		final Config conf = ConfigFactory.load();
		return conf;
	}

	public static List<String> getStorageConfiguration() {
		return getGlobalConfig().getStringList("tabulator.storage.clusterNodes");
	}

	public static List<DataSourceConfiguration> getGloballyConfiguredDataSources() {
		final List<? extends ConfigObject> dss = getGlobalConfig().getObjectList("tabulator.data-sources");
		final List<DataSourceConfiguration> res = new LinkedList<DataSourceConfiguration>();
		for (final ConfigObject o : dss) {
			final Config cfg = o.toConfig();
			final DataSourceConfiguration dsc = new DataSourceConfiguration();
			dsc.setUri(cfg.getString("uri"));
			final int refreshTimeSec = cfg.getInt("refreshTimeSeconds");
			dsc.setRefreshTimeSeconds(refreshTimeSec);
			res.add(dsc);
		}
		return res;
	}

	private static Config getGlobalConfig() {
		if(GLOBAL_CONFIG != null){
			return GLOBAL_CONFIG;
		}
		logger.debug("Reading globally configured data sources...");
		String configFilePath = System.getenv(Constants.CONFIG_FILE_PATH_PROP_NAME);
		if (configFilePath == null || configFilePath.isEmpty()) {
			configFilePath = System.getProperty(Constants.CONFIG_FILE_PATH_PROP_NAME);
		}
		if (configFilePath == null || configFilePath.isEmpty()) {
			throw new IllegalStateException("Was not able to find env variable " + Constants.CONFIG_FILE_PATH_PROP_NAME
					+ ". Did you configure tabulator properly?");
		}
		logger.debug("Will try to read configuration from {}", configFilePath);
		GLOBAL_CONFIG = loadConfig(configFilePath);
		logger.debug("Successfully loaded global config from {}", configFilePath);
		return GLOBAL_CONFIG;
	}

	private static Config loadConfig(String configPath) {
		if (Util.class.getClassLoader().getResourceAsStream(configPath) != null) {
			return ConfigFactory.load(configPath);
		}
		final File f = new File(configPath);
		if (!f.exists() || !f.isFile()) {
			throw new IllegalStateException(configPath + " is not a file!");
		}
		final Config config = ConfigFactory.parseFile(f);
		return config;
	}

}
