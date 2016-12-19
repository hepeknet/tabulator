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

	private static final Config GLOBAL_CONFIG = getGlobalConfig();

	public static Config getInternalConfig() {
		Config conf = ConfigFactory.load();
		return conf;
	}

	public static List<String> getStorageConfiguration() {
		return GLOBAL_CONFIG.getStringList("storage.clusterNodes");
	}

	public static List<DataSourceConfiguration> getGloballyConfiguredDataSources() {
		List<? extends ConfigObject> dss = GLOBAL_CONFIG.getObjectList("tabulator.data-sources");
		List<DataSourceConfiguration> res = new LinkedList<DataSourceConfiguration>();
		for (ConfigObject o : dss) {
			Config cfg = o.toConfig();
			DataSourceConfiguration dsc = new DataSourceConfiguration();
			dsc.setUri(cfg.getString("uri"));
			int refreshTimeSec = cfg.getInt("refreshTimeSeconds");
			dsc.setRefreshTimeSeconds(refreshTimeSec);
			res.add(dsc);
		}
		return res;
	}

	private static Config getGlobalConfig() {
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
		return loadConfig(configFilePath);
	}

	private static Config loadConfig(String configPath) {
		if (Util.class.getClassLoader().getResourceAsStream(configPath) != null) {
			return ConfigFactory.load(configPath);
		}
		File f = new File(configPath);
		if (!f.exists() || !f.isFile()) {
			throw new IllegalStateException(configPath + " is not a file!");
		}
		Config config = ConfigFactory.parseFile(f);
		return config;
	}

}
