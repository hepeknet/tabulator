package net.hepek.tabulator.api.pojo;

public abstract class ColumnInfo {

	private String name;
	private ColumnType type;
	private String fullPath;
	private boolean isOptional;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public ColumnType getType() {
		return type;
	}

	public void setType(ColumnType type) {
		this.type = type;
	}

	public String getFullPath() {
		return fullPath;
	}

	public void setFullPath(String fullPath) {
		this.fullPath = fullPath;
	}

	public boolean isOptional() {
		return isOptional;
	}

	public void setOptional(boolean isOptional) {
		this.isOptional = isOptional;
	}

	@Override
	public String toString() {
		return "ColumnInfo [" + (name != null ? "name=" + name + ", " : "")
				+ (type != null ? "type=" + type + ", " : "") + (fullPath != null ? "fullPath=" + fullPath + ", " : "")
				+ "isOptional=" + isOptional + "]";
	}

}
