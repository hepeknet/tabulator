package net.hepek.fs.impl;

import java.util.List;

import net.hepek.tabulator.Util;

public abstract class ParquetUtil {

	public static List<String> getParquetFileExtensions() {
		return Util.getInternalConfig().getStringList("tabulator.parquet.file-extensions");
	}
	
	public static boolean isHiddenFile(String uri){
		return uri.startsWith(".") || uri.startsWith("_");
	}
	
}
