package net.hepek.tabulator.api.pojo;

public class DirectoryInfo {

	private String absolutePath;
	private long sizeBytes;
	private long timeCreated;
	private int numberOfProcessedFilesInsideDirectory;
	private long summedTotalSizeOfFilesBytesUnderDirectory;
	private int numberOfProcessedFilesUnderDirectory;
	private int numberOfUnprocessedFilesUnderDirectory;
	private int numberOfUnprocessedFilesInsideDirectory;
	private int numberOfDifferentSchemasInsideDirectory;
	private int numberOfDifferentSchemasUnderDirectory;
	private long lastUpdateTime;

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

	public int getNumberOfProcessedFilesInsideDirectory() {
		return numberOfProcessedFilesInsideDirectory;
	}

	public void setNumberOfProcessedFilesInsideDirectory(int countProcessedFilesInsideDirectory) {
		this.numberOfProcessedFilesInsideDirectory = countProcessedFilesInsideDirectory;
	}

	public long getSummedTotalSizeOfFilesBytesUnderDirectory() {
		return summedTotalSizeOfFilesBytesUnderDirectory;
	}

	public void setSummedTotalSizeOfFilesBytesUnderDirectory(long summedTotalSizeOfFilesBytesUnderDirectory) {
		this.summedTotalSizeOfFilesBytesUnderDirectory = summedTotalSizeOfFilesBytesUnderDirectory;
	}

	public int getNumberOfProcessedFilesUnderDirectory() {
		return numberOfProcessedFilesUnderDirectory;
	}

	public void setNumberOfProcessedFilesUnderDirectory(int countProcessedFilesUnderDirectory) {
		this.numberOfProcessedFilesUnderDirectory = countProcessedFilesUnderDirectory;
	}

	public int getNumberOfUnprocessedFilesUnderDirectory() {
		return numberOfUnprocessedFilesUnderDirectory;
	}

	public void setNumberOfUnprocessedFilesUnderDirectory(int countUnprocessedFilesUnderDirectory) {
		this.numberOfUnprocessedFilesUnderDirectory = countUnprocessedFilesUnderDirectory;
	}

	public int getNumberOfUnprocessedFilesInsideDirectory() {
		return numberOfUnprocessedFilesInsideDirectory;
	}

	public void setNumberOfUnprocessedFilesInsideDirectory(int countUnprocessedFilesInsideDirectory) {
		this.numberOfUnprocessedFilesInsideDirectory = countUnprocessedFilesInsideDirectory;
	}

	public int getNumberOfDifferentSchemasInsideDirectory() {
		return numberOfDifferentSchemasInsideDirectory;
	}

	public void setNumberOfDifferentSchemasInsideDirectory(int numberOfDifferentSchemasInsideDirectory) {
		this.numberOfDifferentSchemasInsideDirectory = numberOfDifferentSchemasInsideDirectory;
	}

	public int getNumberOfDifferentSchemasUnderDirectory() {
		return numberOfDifferentSchemasUnderDirectory;
	}

	public void setNumberOfDifferentSchemasUnderDirectory(int numberOfDifferentSchemasUnderDirectory) {
		this.numberOfDifferentSchemasUnderDirectory = numberOfDifferentSchemasUnderDirectory;
	}

	public long getLastUpdateTime() {
		return lastUpdateTime;
	}

	public void setLastUpdateTime(long lastUpdateTime) {
		this.lastUpdateTime = lastUpdateTime;
	}

}
