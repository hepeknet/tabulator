package net.hepek.fs.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.hepek.tabulator.api.ds.DataSource;
import net.hepek.tabulator.api.ds.DataSourceProcessor;
import net.hepek.tabulator.api.pojo.DataSourceInfo;
import net.hepek.tabulator.api.pojo.DataSourceType;
import net.hepek.tabulator.api.pojo.DirectoryInfo;
import net.hepek.tabulator.api.pojo.DirectoryWithSchema;
import net.hepek.tabulator.api.pojo.FileType;
import net.hepek.tabulator.api.pojo.FileWithSchema;
import net.hepek.tabulator.api.pojo.SchemaInfo;
import net.hepek.tabulator.api.storage.Storage;

public class FSDataSourceProcessor implements DataSourceProcessor {

	private static final String HDFS_PREFIX = "hdfs:";
	private static final String FILE_PREFIX = "file:";
	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public boolean understandsDataSourceUri(DataSource ds) {
		if (ds != null && ds.getUri() != null) {
			return ds.getUri().startsWith(FILE_PREFIX) || ds.getUri().startsWith(HDFS_PREFIX)
					|| ds.getUri().startsWith("/");
		}
		return false;
	}

	@Override
	public void processDataSource(DataSource ds, Storage saver) {
		if (ds == null) {
			throw new IllegalArgumentException("DataSource must not be null");
		}
		if (saver == null) {
			throw new IllegalArgumentException("Saver must not be null");
		}
		log.debug("Processing data source {}", ds);
		try {
			processDS(ds, saver);
		} catch (final Exception ie) {
			log.error("Exception while processing {}", ds, ie);
		}
	}

	private void processDS(DataSource ds, Storage storage) throws Exception {
		log.debug("Processing {}", ds);
		final DataSourceInfo dsi = new DataSourceInfo();
		dsi.setAccessURI(ds.getUri());
		FileWrapper root = null;
		FileSystem hdfs = null;
		if (isLocalFs(ds.getUri())) {
			dsi.setType(DataSourceType.LOCAL_FS);
			final Path dir = Paths.get(ds.getUri());
			root = new FileWrapper(dir);
		} else {
			dsi.setType(DataSourceType.HDFS);
			hdfs = new DistributedFileSystem();
			hdfs.initialize(new URI(ds.getUri()), new Configuration());
			final org.apache.hadoop.fs.Path fR = new org.apache.hadoop.fs.Path(ds.getUri());
			final FileStatus dirFS = hdfs.getFileStatus(fR);
			root = new FileWrapper(dirFS, hdfs);
		}
		if (!root.isDirectory()) {
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
		dsi.setTags(ds.getTags());
		dsi.setProperties(ds.getProperties());
		storage.save(dsi);
		log.debug("Successfully processed {}", ds);
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
		int totalFilesUnprocessedInsideDir = 0;
		int totalFilesUnprocessedUnderDirectory = 0;
		long totalFilesSizeBytesUnderDir = 0;
		final FileWrapper[] children = dir.listChildren();
		final Set<String> schemasInsideDir = new HashSet<>();
		final Set<String> schemasUnderDir = new HashSet<>();
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
					totalFilesUnprocessedUnderDirectory += dirOut.countUnprocessedFilesInsideDirectory;
					schemasUnderDir.addAll(dirOut.schemasUnderDir);
				} else {
					try {
						final FileProcessingOutput out = processFile(converter, fw, storage, dsi);
						if (out == null) {
							totalFilesUnprocessedInsideDir += 1;
						} else {
							if (oldestCreationTime < out.timeCreated) {
								oldestCreationTime = out.timeCreated;
							}
							if (out.timeUpdated > lastUpdateTime) {
								lastUpdateTime = out.timeUpdated;
							}
							schemasInsideDir.add(out.fileSchemaId);
							totalSizeInBytes += out.sizeBytes;
							totalFilesProcessedDirectlyInsideDir += 1;
						}
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
		res.countProcessedFilesUnderDirectory = totalFilesProcessedUnderDirectory
				+ totalFilesProcessedDirectlyInsideDir;
		res.countUnprocessedFilesInsideDirectory = totalFilesUnprocessedInsideDir;
		res.countUnprocessedFilesUnderDirectory = totalFilesUnprocessedInsideDir + totalFilesUnprocessedUnderDirectory;
		res.summedTotalSizeOfFilesBytesUnderDirectory = totalFilesSizeBytesUnderDir + totalSizeInBytes;
		res.schemasUnderDir.addAll(schemasUnderDir);
		res.schemasInsideDir.addAll(schemasInsideDir);
		log.debug("In total processed {} files in {}", totalFilesProcessedDirectlyInsideDir, dir.getFullPath());
		final boolean shouldPersist = shouldPersistDirectory(res);
		DirectoryInfo di = null;
		final boolean hasSingleSchema = res.schemasUnderDir.size() == 1;
		if (hasSingleSchema) {
			di = new DirectoryWithSchema();
		} else {
			di = new DirectoryInfo();
		}
		if (shouldPersist) {
			di.setAbsolutePath(dir.getFullPath());
			di.setNumberOfProcessedFilesInsideDirectory(res.countProcessedFilesInsideDirectory);
			di.setNumberOfProcessedFilesUnderDirectory(res.countProcessedFilesUnderDirectory);
			di.setSizeBytes(res.sizeBytes);
			di.setSummedTotalSizeOfFilesBytesUnderDirectory(res.summedTotalSizeOfFilesBytesUnderDirectory);
			di.setTimeCreated(res.timeCreated);
			di.setNumberOfDifferentSchemasInsideDirectory(res.schemasInsideDir.size());
			di.setNumberOfDifferentSchemasUnderDirectory(res.schemasUnderDir.size());
			di.setNumberOfUnprocessedFilesInsideDirectory(res.countUnprocessedFilesInsideDirectory);
			di.setNumberOfProcessedFilesUnderDirectory(res.countUnprocessedFilesUnderDirectory);
			di.setLastUpdateTime(res.timeUpdated);
			if (hasSingleSchema) {
				final String absPath = dir.getFullPath();
				final String schemaId = res.schemasUnderDir.iterator().next();
				final DirectoryWithSchema dws = (DirectoryWithSchema) di;
				dws.setSchemaId(schemaId);
				log.debug("Persisting directory {} with single assigned schema {}", absPath, schemaId);
				storage.save(dws);
			} else {
				storage.save(di);
			}
		}
		saveLastModificationTime(dir, storage);
		return res;
	}

	private boolean shouldPersistDirectory(DirectoryProcessingOutput dpo) {
		return dpo.schemasUnderDir.size() > 0 || dpo.countUnprocessedFilesUnderDirectory > 0;
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
			} else {
				return null;
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
			out.fileSchemaId = fws.getSchemaId();
			fws.setType(FileType.PARQUET);
			fws.setAbsolutePath(fw.getFullPath());
			fws.setSchemaId(si.getId());
			storage.save(fws);
			log.trace("Successfully parsed {}", fw.getFullPath());
		}
		return out;
	}

	class FileProcessingOutput {
		String fileSchemaId;
		long timeCreated;
		long timeUpdated;
		long sizeBytes;
		// size of file or (for dirs) size of all files inside dir
	}

	class DirectoryProcessingOutput extends FileProcessingOutput {
		int countProcessedFilesInsideDirectory;
		long summedTotalSizeOfFilesBytesUnderDirectory;
		int countProcessedFilesUnderDirectory;
		int countUnprocessedFilesInsideDirectory;
		int countUnprocessedFilesUnderDirectory;
		Set<String> schemasUnderDir = new HashSet<>();
		Set<String> schemasInsideDir = new HashSet<>();
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
