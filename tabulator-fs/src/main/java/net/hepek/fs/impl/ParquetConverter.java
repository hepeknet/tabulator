package net.hepek.fs.impl;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import net.hepek.tabulator.api.pojo.BooleanColumnInfo;
import net.hepek.tabulator.api.pojo.ColumnInfo;
import net.hepek.tabulator.api.pojo.GroupColumnInfo;
import net.hepek.tabulator.api.pojo.NumericalColumnInfo;
import net.hepek.tabulator.api.pojo.SchemaInfo;
import net.hepek.tabulator.api.pojo.StringColumnInfo;

public class ParquetConverter {
	
	public SchemaInfo parseParquetFile(URI uri) throws IOException{
		final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(uri);
		final ParquetMetadata pmd = ParquetFileReader.readFooter(new Configuration(), path, ParquetMetadataConverter.NO_FILTER);
		final SchemaInfo si = new SchemaInfo();
		
		final org.apache.parquet.hadoop.metadata.FileMetaData fmd = pmd.getFileMetaData();
		final String createdBy = fmd.getCreatedBy();
		si.setCreatedBy(createdBy);
		// TODO add avro here etc
		si.setType("parquet");
		final MessageType schema = fmd.getSchema();
		final String name = schema.getName();
		si.setName(name);
		
		final List<ColumnInfo> cols = new LinkedList<>();
		for(final Type field : schema.getFields()) {
			final ColumnInfo ci = convertField(field, null);
			cols.add(ci);
		}
		si.setColumns(cols);
		return si;
	}
	
	private ColumnInfo convertField(Type field, String parentFullPath) {
		ColumnInfo ci = null;
		String fullPath = field.getName();
		if(parentFullPath != null){
			fullPath = parentFullPath + "." + field.getName();
		}
		if(field.isPrimitive()){
			final PrimitiveType pm = field.asPrimitiveType();
			if(PrimitiveTypeName.BOOLEAN == pm.getPrimitiveTypeName()) {
				ci = new BooleanColumnInfo();
			} else if(PrimitiveTypeName.BINARY == pm.getPrimitiveTypeName()){
				ci = new StringColumnInfo();
			} else {
				ci = new NumericalColumnInfo();
			}
			ci.setOptional(pm.getRepetition() == Repetition.OPTIONAL);
		} else {
			ci = new GroupColumnInfo();
			final GroupType gt = field.asGroupType();
			final List<ColumnInfo> children = new LinkedList<>();
			for(final Type t : gt.getFields()){
				final ColumnInfo c = convertField(t, fullPath);
				children.add(c);
			}
			((GroupColumnInfo)ci).setChildren(children);
		}
		ci.setName(field.getName());
		ci.setFullPath(fullPath);
		return ci;
	}

}
