package net.hepek.tabulator.api.ds;

import java.util.Map;
import java.util.Set;

public class DataSource {

	private String uri;
	private Set<String> tags;
	private Map<String, String> properties;

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public Set<String> getTags() {
		return tags;
	}

	public void setTags(Set<String> tags) {
		this.tags = tags;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

}
