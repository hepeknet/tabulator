package net.hepek.fs.impl;

import java.net.URI;

import org.junit.Assert;
import org.junit.Test;

import net.hepek.tabulator.api.ds.DataSource;
import net.hepek.tabulator.api.pojo.SchemaInfo;

public class TestParquetParsing {

	@Test
	public void test_basic() {
		Assert.assertFalse(new FSDataSourceProcessor().understandsDataSourceUri(null));
		final DataSource ds = new DataSource();
		ds.setUri("");
		Assert.assertFalse(new FSDataSourceProcessor().understandsDataSourceUri(ds));
		ds.setUri("jdbc");
		Assert.assertFalse(new FSDataSourceProcessor().understandsDataSourceUri(ds));
		ds.setUri("hdfs://abc/efg/abc");
		Assert.assertTrue(new FSDataSourceProcessor().understandsDataSourceUri(ds));
		ds.setUri("file:///abc/ddd/ccc");
		Assert.assertTrue(new FSDataSourceProcessor().understandsDataSourceUri(ds));
		ds.setUri("/a/b/c/d");
		Assert.assertTrue(new FSDataSourceProcessor().understandsDataSourceUri(ds));
	}
	
	@Test
	public void test_one() throws Exception {
		final URI nationParquetURI = this.getClass().getResource("/parquet/nation.parquet").toURI();
		final SchemaInfo si = new ParquetConverter().parseParquetFile(nationParquetURI);
		System.out.println(si);
	}
}