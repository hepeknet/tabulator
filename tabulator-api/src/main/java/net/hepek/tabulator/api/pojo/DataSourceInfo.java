package net.hepek.tabulator.api.pojo;

import java.util.Map;
import java.util.Set;

public class DataSourceInfo {

	private String accessURI;
	private long oldestItemCreationTime;
	private long lastUpdateTime;
	private long sizeInBytes;
	private Set<String> tags;
	private DataSourceType type;
	private Map<String, String> properties;

	public String getAccessURI() {
		return accessURI;
	}

	public void setAccessURI(String accessURI) {
		this.accessURI = accessURI;
	}

	public long getOldestItemCreationTime() {
		return oldestItemCreationTime;
	}

	public void setOldestItemCreationTime(long oldestItemCreationTime) {
		this.oldestItemCreationTime = oldestItemCreationTime;
	}

	public long getLastUpdateTime() {
		return lastUpdateTime;
	}

	public void setLastUpdateTime(long lastUpdateTime) {
		this.lastUpdateTime = lastUpdateTime;
	}

	public long getSizeInBytes() {
		return sizeInBytes;
	}

	public void setSizeInBytes(long sizeInBytes) {
		this.sizeInBytes = sizeInBytes;
	}

	public Set<String> getTags() {
		return tags;
	}

	public void setTags(Set<String> tags) {
		this.tags = tags;
	}

	public DataSourceType getType() {
		return type;
	}

	public void setType(DataSourceType type) {
		this.type = type;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	@Override
	public String toString() {
		return "DataSourceInfo [" + (accessURI != null ? "accessURI=" + accessURI + ", " : "")
				+ "oldestItemCreationTime=" + oldestItemCreationTime + ", lastUpdateTime=" + lastUpdateTime
				+ ", sizeInBytes=" + sizeInBytes + ", " + (tags != null ? "tags=" + tags + ", " : "")
				+ (type != null ? "type=" + type + ", " : "") + (properties != null ? "properties=" + properties : "")
				+ "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((accessURI == null) ? 0 : accessURI.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final DataSourceInfo other = (DataSourceInfo) obj;
		if (accessURI == null) {
			if (other.accessURI != null)
				return false;
		} else if (!accessURI.equals(other.accessURI))
			return false;
		return true;
	}

}
