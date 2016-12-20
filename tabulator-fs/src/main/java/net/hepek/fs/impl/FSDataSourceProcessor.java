package net.hepek.fs.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.hepek.tabulator.api.ds.DataSourceProcessor;
import net.hepek.tabulator.api.pojo.DataSourceInfo;
import net.hepek.tabulator.api.pojo.DataSourceType;
import net.hepek.tabulator.api.pojo.FileType;
import net.hepek.tabulator.api.pojo.FileWithSchema;
import net.hepek.tabulator.api.pojo.SchemaInfo;
import net.hepek.tabulator.api.storage.PojoSaver;

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
	public void processDataSource(String uri, PojoSaver saver) {
		if (saver == null) {
			throw new IllegalArgumentException("Saver must not be null");
		}
		log.debug("Processing data source {}", uri);
		try {
			if (isLocalFs(uri)) {
				processLocalDS(uri, saver);
			} else {
				processHDFS(uri, saver);
			}
		} catch (final IOException ie) {
			log.error("Exception while processing {}", uri, ie);
		}
	}

	private void processLocalDS(String uri, PojoSaver saver) throws IOException {
		log.debug("Processing local fs {}", uri);
		final DataSourceInfo dsi = new DataSourceInfo();
		dsi.setAccessURI(uri);
		dsi.setType(DataSourceType.FS);
		final Path dir = Paths.get(uri);
		final ParquetConverter converter = new ParquetConverter();
		long totalSizeInBytes = 0;
		long oldestCreationTime = 0;
		long lastUpdateTime = 0;
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.{parquet, pq}")) {
			for (final Path entry : stream) {
				final File f = entry.toFile();
				if (f.isDirectory()) {
					// TODO
				} else {
					try {
						final SchemaInfo si = converter.parseParquetFile(entry.toUri());
						si.addDatasource(dsi);
						saver.save(si);
						final FileWithSchema fws = new FileWithSchema();
						final BasicFileAttributes attrs = Files.readAttributes(entry, BasicFileAttributes.class);
						final long timeCreated = attrs.creationTime().toMillis();
						final long timeUpdated = attrs.lastModifiedTime().toMillis();
						fws.setSizeBytes(f.length());
						fws.setTimeCreated(timeCreated);
						if (oldestCreationTime < timeCreated) {
							oldestCreationTime = timeCreated;
						}
						if (timeUpdated > lastUpdateTime) {
							lastUpdateTime = timeUpdated;
						}
						totalSizeInBytes += f.length();

						fws.setType(FileType.PARQUET);
						fws.setAbsolutePath(f.getAbsolutePath());
						fws.setSchemaId(si.getId());
						saver.save(fws);
						log.debug("Successfully parsed {}", entry);
					} catch (final Exception exc) {
						log.warn("Exception while parsing {} - details {}", entry, exc.getMessage());
					}
				}
			}
		} catch (final DirectoryIteratorException ex) {
			throw ex.getCause();
		}
		dsi.setSizeInBytes(totalSizeInBytes);
		dsi.setOldestItemCreationTime(oldestCreationTime);
		dsi.setLastUpdateTime(lastUpdateTime);
		saver.save(dsi);
		log.debug("Successfully processed {}", uri);
	}

	private void processHDFS(String uri, PojoSaver saver) {

	}

	private static boolean isLocalFs(String uri) {
		return uri.startsWith(FILE_PREFIX) || uri.startsWith("/");
	}

}
