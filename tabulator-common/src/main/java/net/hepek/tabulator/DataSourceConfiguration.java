package net.hepek.tabulator;

import java.util.Map;
import java.util.Set;

public class DataSourceConfiguration {

	private String uri;
	private int refreshTimeSeconds;
	private Set<String> tags;
	private Map<String, String> properties;

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public int getRefreshTimeSeconds() {
		return refreshTimeSeconds;
	}

	public void setRefreshTimeSeconds(int refreshTimeSeconds) {
		if (refreshTimeSeconds < 0) {
			throw new IllegalArgumentException("Refresh must be >= 0");
		}
		this.refreshTimeSeconds = refreshTimeSeconds;
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
