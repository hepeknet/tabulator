package net.hepek.tabulator.api.pojo;

public class DirectoryInfo {

	private String absolutePath;
	private long sizeBytes;
	private long timeCreated;
	private int countProcessedFilesInsideDirectory;
	private long summedTotalSizeOfFilesBytesUnderDirectory;
	private int countProcessedFilesUnderDirectory;
	private int countUnprocessedFilesUnderDirectory;
	private int countUnprocessedFilesInsideDirectory;
	private int numberOfDifferentSchemasInsideDirectory;
	private int numberOfDifferentSchemasUnderDirectory;

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

	public int getCountProcessedFilesInsideDirectory() {
		return countProcessedFilesInsideDirectory;
	}

	public void setCountProcessedFilesInsideDirectory(int countProcessedFilesInsideDirectory) {
		this.countProcessedFilesInsideDirectory = countProcessedFilesInsideDirectory;
	}

	public long getSummedTotalSizeOfFilesBytesUnderDirectory() {
		return summedTotalSizeOfFilesBytesUnderDirectory;
	}

	public void setSummedTotalSizeOfFilesBytesUnderDirectory(long summedTotalSizeOfFilesBytesUnderDirectory) {
		this.summedTotalSizeOfFilesBytesUnderDirectory = summedTotalSizeOfFilesBytesUnderDirectory;
	}

	public int getCountProcessedFilesUnderDirectory() {
		return countProcessedFilesUnderDirectory;
	}

	public void setCountProcessedFilesUnderDirectory(int countProcessedFilesUnderDirectory) {
		this.countProcessedFilesUnderDirectory = countProcessedFilesUnderDirectory;
	}

	public int getCountUnprocessedFilesUnderDirectory() {
		return countUnprocessedFilesUnderDirectory;
	}

	public void setCountUnprocessedFilesUnderDirectory(int countUnprocessedFilesUnderDirectory) {
		this.countUnprocessedFilesUnderDirectory = countUnprocessedFilesUnderDirectory;
	}

	public int getCountUnprocessedFilesInsideDirectory() {
		return countUnprocessedFilesInsideDirectory;
	}

	public void setCountUnprocessedFilesInsideDirectory(int countUnprocessedFilesInsideDirectory) {
		this.countUnprocessedFilesInsideDirectory = countUnprocessedFilesInsideDirectory;
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

}
