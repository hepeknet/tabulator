package net.hepek.tabulator.api.pojo;

public class FileWithSchema {

	private String schemaId;
	private String absolutePath;
	private long sizeBytes;
	private long timeCreated;
	private FileType type;

	public String getSchemaId() {
		return schemaId;
	}

	public void setSchemaId(String schemaId) {
		this.schemaId = schemaId;
	}

	public String getAbsolutePath() {
		return absolutePath;
	}

	public void setAbsolutePath(String absolutePath) {
		this.absolutePath = absolutePath;
	}

	public long getSizeBytes() {
		return sizeBytes;
	}

	public void setSizeBytes(long sizeBytes) {
		this.sizeBytes = sizeBytes;
	}

	public long getTimeCreated() {
		return timeCreated;
	}

	public void setTimeCreated(long timeCreated) {
		this.timeCreated = timeCreated;
	}

	public FileType getType() {
		return type;
	}

	public void setType(FileType type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "FileWithSchema [" + (schemaId != null ? "schemaId=" + schemaId + ", " : "")
				+ (absolutePath != null ? "absolutePath=" + absolutePath + ", " : "") + "sizeBytes=" + sizeBytes
				+ ", timeCreated=" + timeCreated + ", " + (type != null ? "type=" + type : "") + "]";
	}

}
