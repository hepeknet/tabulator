package net.hepek.tabulator.api.pojo;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemaInfo {

	private String id;
	private String name;
	private String createdBy;
	private String type;
	private Map<String, String> properties;
	private Set<String> tags;
	private List<ColumnInfo> columns;
	private final Set<DataSourceInfo> datasources = new HashSet<DataSourceInfo>();

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, String> properties) {
		this.properties = properties;
	}

	public Set<String> getTags() {
		return tags;
	}

	public void setTags(Set<String> tags) {
		this.tags = tags;
	}

	public List<ColumnInfo> getColumns() {
		return columns;
	}

	public void setColumns(List<ColumnInfo> columns) {
		this.columns = columns;
	}

	public Set<DataSourceInfo> getDatasources() {
		return datasources;
	}

	public void addDatasource(DataSourceInfo ds) {
		this.datasources.add(ds);
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "SchemaInfo [" + (id != null ? "id=" + id + ", " : "") + (name != null ? "name=" + name + ", " : "")
				+ (createdBy != null ? "createdBy=" + createdBy + ", " : "")
				+ (properties != null ? "properties=" + properties + ", " : "")
				+ (tags != null ? "tags=" + tags + ", " : "") + (columns != null ? "columns=" + columns + ", " : "")
				+ (datasources != null ? "datasources=" + datasources : "") + "]";
	}

}
