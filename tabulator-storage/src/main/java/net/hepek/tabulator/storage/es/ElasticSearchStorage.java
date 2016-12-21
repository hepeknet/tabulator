package net.hepek.tabulator.storage.es;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.hepek.tabulator.api.pojo.DataSourceInfo;
import net.hepek.tabulator.api.pojo.FileWithSchema;
import net.hepek.tabulator.api.pojo.SchemaInfo;
import net.hepek.tabulator.api.storage.Storage;

public class ElasticSearchStorage implements Storage {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private static final int DEFAULT_PORT = 9300;

	private static final String SCHEMA_INDEX = "schema";
	private static final String DS_INDEX = "datasource";
	private static final String FILES_INDEX = "files";

	private static final String INTERNAL_MODIFICATION_INDEX = "tab_int_modifications";
	private static final String INTERNAL_MODIFICATION_TYPE = "tab_int_modifications_type";

	private final org.elasticsearch.client.transport.TransportClient client;

	private final ObjectMapper mapper = new ObjectMapper();

	public ElasticSearchStorage(List<String> clusterNodes) {
		if (clusterNodes == null || clusterNodes.isEmpty()) {
			throw new IllegalArgumentException("Cluster nodes must not be null or empty");
		}
		logger.debug("Cluster nodes are {}", clusterNodes);
		client = new PreBuiltTransportClient(Settings.EMPTY);
		for (final String addr : clusterNodes) {
			logger.debug("Parsing {}", addr);
			final int indexOfColon = addr.indexOf(':');
			String host = addr;
			int port = DEFAULT_PORT;
			if (indexOfColon > 0) {
				host = addr.substring(0, indexOfColon);
				logger.debug("Host is {}", host);
				final String portStr = addr.substring(indexOfColon + 1);
				logger.debug("Port is {}", portStr);
				port = Integer.parseInt(portStr);
			}
			logger.debug("Adding ES host {}:{}", host, port);
			try {
				client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
				logger.debug("Successfully created client to {}:{}", host, port);
			} catch (final UnknownHostException e) {
				throw new IllegalStateException("Unable to access ES host - given string is [" + addr + "]");
			}
		}
	}

	@Override
	public void save(SchemaInfo schemaInfo) {
		try {
			final byte[] json = mapper.writeValueAsBytes(schemaInfo);
			final IndexResponse ir = client.prepareIndex().setSource(json).setIndex(SCHEMA_INDEX)
					.setType(schemaInfo.getType()).setId(schemaInfo.getId()).get();
		} catch (final JsonProcessingException e) {
			logger.warn("Was not able to save schema. The reason is ", e);
		}

	}

	@Override
	public void save(FileWithSchema file) {
		try {
			final byte[] json = mapper.writeValueAsBytes(file);
			final IndexResponse ir = client.prepareIndex().setSource(json).setIndex(FILES_INDEX)
					.setType(file.getType().name()).setId(file.getAbsolutePath()).get();
		} catch (final JsonProcessingException e) {
			logger.warn("Was not able to save schema. The reason is ", e);
		}
	}

	@Override
	public void save(DataSourceInfo ds) {
		try {
			final byte[] json = mapper.writeValueAsBytes(ds);
			final IndexResponse ir = client.prepareIndex().setSource(json).setIndex(DS_INDEX)
					.setType(ds.getType().name()).setId(ds.getAccessURI()).get();
		} catch (final JsonProcessingException e) {
			logger.warn("Was not able to save schema. The reason is ", e);
		}
	}

	@Override
	public void close() {
		if (this.client != null) {
			logger.debug("Closing client connection...");
			this.client.close();
		}
	}

	@Override
	public long getLastModified(String path) {
		try {
			if (path == null || path.isEmpty()) {
				throw new IllegalArgumentException("Path must not be null or empty!");
			}
			final GetResponse response = client
					.prepareGet(INTERNAL_MODIFICATION_INDEX, INTERNAL_MODIFICATION_TYPE, path).get();
			if (response.isExists()) {
				final byte[] bytes = response.getSourceAsBytes();
				final InternalModificationTime imt = mapper.readerFor(InternalModificationTime.class).readValue(bytes);
				logger.debug("Last modification time {}={}", path, imt.getLastModificationTime());
				return imt.getLastModificationTime();
			} else {
				logger.debug("Was not able to find last modification time for {}", path);
			}
		} catch (final Exception exc) {
			logger.warn("Exception while trying to get last modifiaction time for {}", path, exc);
		}
		return 0;
	}

	@Override
	public void saveLastModified(String path, long modificationTime) {
		if (path == null || path.isEmpty()) {
			throw new IllegalArgumentException("Path must not be null or empty!");
		}
		if (modificationTime < 0) {
			throw new IllegalArgumentException("Modifiation time must not be < 0");
		}
		final InternalModificationTime imt = new InternalModificationTime();
		imt.setLastModificationTime(modificationTime);
		imt.setUri(path);
		try {
			final byte[] json = mapper.writeValueAsBytes(imt);
			final IndexResponse ir = client.prepareIndex().setSource(json).setIndex(INTERNAL_MODIFICATION_INDEX)
					.setType(INTERNAL_MODIFICATION_TYPE).setId(path).get();
			logger.debug("Saved modification time for {}={}", path, modificationTime);
		} catch (final JsonProcessingException e) {
			logger.warn("Was not able to save schema. The reason is ", e);
		}
	}

}
