package net.hepek.tabulator.storage.es;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

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
import net.hepek.tabulator.api.storage.PojoSaver;

public class ElasticSearchSaver implements PojoSaver {
	
	private final Logger logger = LoggerFactory.getLogger(getClass());
	
	private static final int DEFAULT_PORT = 9300;
	
	private org.elasticsearch.client.transport.TransportClient client;
	
	private final ObjectMapper mapper = new ObjectMapper();
	
	public ElasticSearchSaver(List<String> clusterNodes) {
		if(clusterNodes == null || clusterNodes.isEmpty()){
			throw new IllegalArgumentException("Cluster nodes must not be null or empty");
		}
		client = new PreBuiltTransportClient(Settings.EMPTY);
		for(String addr : clusterNodes){
			int indexOfColon = addr.indexOf(':');
			String host = "";
			int port = DEFAULT_PORT;
			if(indexOfColon > 0){
				host = addr.substring(0, indexOfColon);
				port = Integer.parseInt(addr.substring(indexOfColon + 1));
			}
			logger.debug("Adding ES host {}:{}", host, port);
			try {
				client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
			} catch (UnknownHostException e) {
				throw new IllegalStateException("Unable to parse address to ES - given string is [" + addr + "]");
			}
		}
		        
	}
	
	public void save(SchemaInfo schemaInfo) {
		try {
			byte[] json = mapper.writeValueAsBytes(schemaInfo);
			IndexResponse ir = client.prepareIndex().setSource(json).get();
		} catch (JsonProcessingException e) {
			logger.warn("Was not able to save schema. The reason is ", e);
		}
		
	}

	public void save(FileWithSchema file) {
		try {
			byte[] json = mapper.writeValueAsBytes(file);
			IndexResponse ir = client.prepareIndex().setSource(json).get();
		} catch (JsonProcessingException e) {
			logger.warn("Was not able to save schema. The reason is ", e);
		}
	}

	public void save(DataSourceInfo ds) {
		try {
			byte[] json = mapper.writeValueAsBytes(ds);
			IndexResponse ir = client.prepareIndex().setSource(json).get();
		} catch (JsonProcessingException e) {
			logger.warn("Was not able to save schema. The reason is ", e);
		}
	}
	
	public void close(){
		if(this.client != null){
			logger.debug("Closing client connection...");
			this.client.close();
		}
	}

}
