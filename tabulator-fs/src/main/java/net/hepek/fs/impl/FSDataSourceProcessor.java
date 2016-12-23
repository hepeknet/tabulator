package net.hepek.fs.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.hepek.tabulator.api.ds.DataSourceProcessor;
import net.hepek.tabulator.api.pojo.DataSourceInfo;
import net.hepek.tabulator.api.pojo.DataSourceType;
import net.hepek.tabulator.api.pojo.DirectoryInfo;
import net.hepek.tabulator.api.pojo.FileType;
import net.hepek.tabulator.api.pojo.FileWithSchema;
import net.hepek.tabulator.api.pojo.SchemaInfo;
import net.hepek.tabulator.api.storage.Storage;

public class FSDataSourceProcessor implements DataSourceProcessor {

	private static final String HDFS_PREFIX = "hdfs:";
	private static final String FILE_PREFIX = "file:";
	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public boolean understandsDataSourceUri(String uri) {
		if (uri != null) {
			return uri.startsWith(FILE_PREFIX) || uri.startsWith(HDFS_PREFIX) || uri.startsWith("/");
		}
		return false;
	}

	@Override
	public void processDataSource(String uri, Storage saver) {
		if (saver == null) {
			throw new IllegalArgumentException("Saver must not be null");
		}
		log.debug("Processing data source {}", uri);
		try {
			processDS(uri, saver);
		} catch (final Exception ie) {
			log.error("Exception while processing {}", uri, ie);
		}
	}

	private void processDS(String uri, Storage storage) throws Exception {
		log.debug("Processing {}", uri);
		final DataSourceInfo dsi = new DataSourceInfo();
		dsi.setAccessURI(uri);
		FileWrapper root = null;
		FileSystem hdfs = null;
		if (isLocalFs(uri)) {
			dsi.setType(DataSourceType.LOCAL_FS);
			final Path dir = Paths.get(uri);
			root = new FileWrapper(dir);
		} else {
			dsi.setType(DataSourceType.HDFS);
			hdfs = new DistributedFileSystem();
			hdfs.initialize(new URI(uri), new Configuration());
			final org.apache.hadoop.fs.Path fR = new org.apache.hadoop.fs.Path(uri);
			final FileStatus dirFS = hdfs.getFileStatus(fR);
			root = new FileWrapper(dirFS, hdfs);
		}
		if(!root.isDirectory()){
			log.warn("{} is not directory - nothing to process here", root.getFullPath());
			return;
		}
		final ParquetConverter converter = new ParquetConverter();
		final DirectoryProcessingOutput res = processDirectory(root, storage, converter, dsi);
		final long totalSizeInBytes = res.sizeBytes;
		final long oldestCreationTime = res.timeCreated;
		final long lastUpdateTime = res.timeUpdated;
		dsi.setSizeInBytes(totalSizeInBytes);
		dsi.setOldestItemCreationTime(oldestCreationTime);
		dsi.setLastUpdateTime(lastUpdateTime);
		storage.save(dsi);
		log.debug("Successfully processed {}", uri);
		if (hdfs != null) {
			hdfs.close();
		}
	}

	private DirectoryProcessingOutput processDirectory(FileWrapper dir, Storage storage, ParquetConverter converter,
			DataSourceInfo dsi) throws IOException {
		long totalSizeInBytes = 0;
		long oldestCreationTime = 0;
		long lastUpdateTime = 0;
		final DirectoryProcessingOutput res = new DirectoryProcessingOutput();
		final boolean shouldProcessDir = shouldProcessDirectory(dir, storage);
		if (!shouldProcessDir) {
			log.debug("Directory {} was not modified since last check. Will not process it", dir.getFullPath());
			return res;
		}
		int totalFilesProcessedDirectlyInsideDir = 0;
		int totalFilesProcessedUnderDirectory = 0;
		long totalFilesSizeBytesUnderDir = 0;
		final FileWrapper[] children = dir.listChildren();
		if (children != null && children.length > 0) {
			for (final FileWrapper fw : children) {
				if (fw.isDirectory()) {
					final DirectoryProcessingOutput dirOut = processDirectory(fw, storage, converter, dsi);
					if (oldestCreationTime < dirOut.timeCreated) {
						oldestCreationTime = dirOut.timeCreated;
					}
					if (dirOut.timeUpdated > lastUpdateTime) {
						lastUpdateTime = dirOut.timeUpdated;
					}
					totalFilesSizeBytesUnderDir += dirOut.sizeBytes;
					totalFilesProcessedUnderDirectory += dirOut.countProcessedFilesInsideDirectory;
				} else {
					try {
						final FileProcessingOutput out = processFile(converter, fw, storage, dsi);
						if (oldestCreationTime < out.timeCreated) {
							oldestCreationTime = out.timeCreated;
						}
						if (out.timeUpdated > lastUpdateTime) {
							lastUpdateTime = out.timeUpdated;
						}
						totalSizeInBytes += out.sizeBytes;
						totalFilesProcessedDirectlyInsideDir += 1;
					} catch (final Exception exc) {
						log.warn("Exception while parsing file {}", fw.getFullPath(), exc);
					}
				}
			}
		}
		res.sizeBytes = totalSizeInBytes;
		res.timeCreated = oldestCreationTime;
		res.timeUpdated = lastUpdateTime;
		res.countProcessedFilesInsideDirectory = totalFilesProcessedDirectlyInsideDir;
		res.countProcessedFilesUnderDirectory = totalFilesProcessedUnderDirectory + totalFilesProcessedDirectlyInsideDir;
		res.summedTotalSizeOfFilesBytesUnderDirectory = totalFilesSizeBytesUnderDir + totalSizeInBytes;
		log.debug("In total processed {} files in {}", totalFilesProcessedDirectlyInsideDir, dir.getFullPath());
		if(res.countProcessedFilesInsideDirectory > 0){
			final DirectoryInfo di = new DirectoryInfo();
			di.setAbsolutePath(dir.getFullPath());
			di.setCountProcessedFilesInsideDirectory(res.countProcessedFilesInsideDirectory);
			di.setCountProcessedFilesUnderDirectory(res.countProcessedFilesUnderDirectory);
			di.setSizeBytes(res.sizeBytes);
			di.setSummedTotalSizeOfFilesBytesUnderDirectory(res.summedTotalSizeOfFilesBytesUnderDirectory);
			di.setTimeCreated(res.timeCreated);
			storage.save(di);
		}
		saveLastModificationTime(dir, storage);
		return res;
	}

	private void saveLastModificationTime(FileWrapper fw, Storage storage) throws IOException {
		final long dirModificationTime = fw.getLastModificationTime();
		final String dirPath = fw.getFullPath();
		storage.saveLastModified(dirPath, dirModificationTime);
		log.debug("Saved last modification time {}={}", dirPath, dirModificationTime);
	}

	private boolean shouldProcessDirectory(FileWrapper fw, Storage storage) throws IOException {
		if (fw.isHidden()) {
			log.debug("{} is hidden - will not process it", fw.getNameOnly());
			return false;
		}
		final long dirModificationTime = fw.getLastModificationTime();
		final String fullDirPath = fw.getFullPath();
		final long lastRememberedModificationTime = storage.getLastModified(fullDirPath);
		final boolean shouldProcess = lastRememberedModificationTime < dirModificationTime;
		log.debug("Should process {} = {}", fullDirPath, shouldProcess);
		return shouldProcess;
	}

	private FileProcessingOutput processFile(ParquetConverter converter, FileWrapper fw, Storage storage,
			DataSourceInfo dsi) throws Exception {
		final FileProcessingOutput out = new FileProcessingOutput();
		SchemaInfo si = null;
		try {
			final URI uri = fw.toURI();
			final boolean isParquetFile = isParquetFile(uri.toString());
			if (isParquetFile) {
				si = converter.parseParquetFile(uri);
			}
		} catch (final Exception exc) {
			log.warn("Exception while parsing {} - details {}", fw.getFullPath(), exc.getMessage());
		}
		if (si != null) {
			si.addDatasource(dsi);
			storage.save(si);
			final FileWithSchema fws = new FileWithSchema();
			final long timeCreated = fw.getCreationTime();
			final long timeUpdated = fw.getLastModificationTime();
			fws.setSizeBytes(fw.getFileSize());
			fws.setTimeCreated(timeCreated);
			out.timeCreated = timeCreated;
			out.timeUpdated = timeUpdated;
			out.sizeBytes = fw.getFileSize();
			fws.setType(FileType.PARQUET);
			fws.setAbsolutePath(fw.getFullPath());
			fws.setSchemaId(si.getId());
			storage.save(fws);
			log.trace("Successfully parsed {}", fw.getFullPath());
		}
		return out;
	}

	class FileProcessingOutput {
		long timeCreated;
		long timeUpdated;
		long sizeBytes; // size of file or (for dirs) size of all files inside dir
	}

	class DirectoryProcessingOutput extends FileProcessingOutput {
		int countProcessedFilesInsideDirectory;
		long summedTotalSizeOfFilesBytesUnderDirectory;
		int countProcessedFilesUnderDirectory;
	}

	private static boolean isLocalFs(String uri) {
		return uri.startsWith(FILE_PREFIX) || uri.startsWith("/");
	}

	private static boolean isParquetFile(String uri) {
		if (uri.startsWith(".")) {
			return false;
		}
		return uri.endsWith(".parquet") || uri.endsWith(".pqt") || uri.endsWith(".pq");
	}

}
