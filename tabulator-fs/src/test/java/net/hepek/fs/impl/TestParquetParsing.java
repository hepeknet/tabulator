package net.hepek.fs.impl;

import java.net.URI;

import org.junit.Assert;
import org.junit.Test;

import net.hepek.tabulator.api.pojo.SchemaInfo;

public class TestParquetParsing {

	@Test
	public void test_basic() {
		Assert.assertFalse(new FSDataSourceProcessor().understandsDataSourceUri(null));
		Assert.assertFalse(new FSDataSourceProcessor().understandsDataSourceUri(""));
		Assert.assertFalse(new FSDataSourceProcessor().understandsDataSourceUri("jdbc"));
		
		Assert.assertTrue(new FSDataSourceProcessor().understandsDataSourceUri("hdfs://abc/efg/abc"));
		Assert.assertTrue(new FSDataSourceProcessor().understandsDataSourceUri("file:///abc/ddd/ccc"));
		Assert.assertTrue(new FSDataSourceProcessor().understandsDataSourceUri("/a/b/c/d"));
	}
	
	@Test
	public void test_one() throws Exception {
		URI nationParquetURI = this.getClass().getResource("/parquet/nation.parquet").toURI();
		SchemaInfo si = new ParquetConverter().parseParquetFile(nationParquetURI);
		System.out.println(si);
	}
}