package com.test.lucene;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

/**
 * 创建索引 Lucene 3.0+
 * 
 * @author Administrator
 * 
 */
public class Indexer {

	/**
	 * @param args
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {

//		TikaConfig tc ;
		String dateDir = "E:\\temp\\ls";
		String indexDir = "E:\\temp\\li";

		// 创建Directory对象
		Directory dir = new SimpleFSDirectory(new File(indexDir));

		Analyzer luceneAnalyzer = new StandardAnalyzer(Version.LUCENE_32);
		IndexWriter indexWriter = new IndexWriter(dir, luceneAnalyzer, true,
				IndexWriter.MaxFieldLength.LIMITED);
		File[] textFiles = new File(dateDir).listFiles();

		for (int i = 0; i < textFiles.length; i++) {
	
				System.out.println("File " + textFiles[i].getPath()
						+ "正在被索引.");
				String temp = FileReaderAll(textFiles[i].getCanonicalPath(),
						"GBK");
				System.out.println(temp);
				Document document = new Document();
				Field FieldPath = new Field("path", textFiles[i].getPath(),
						Field.Store.YES, Field.Index.NOT_ANALYZED);
				Field FieldBody = new Field("body", temp, Field.Store.YES,
						Field.Index.NOT_ANALYZED);
				document.add(FieldPath);
				document.add(FieldBody);
				indexWriter.addDocument(document);
		}
		// optimize()方法是对索引进行优化
		indexWriter.optimize();
		indexWriter.close();

		// 查看IndexWriter里面有多少个索引
		System.out.println("numDocs" + indexWriter.numDocs());
		indexWriter.close();

	}

	public static String FileReaderAll(String FileName, String charset)
			throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new FileInputStream(FileName), charset));
		String line = new String();
		String temp = new String();

		while ((line = reader.readLine()) != null) {
			temp += line;
		}
		reader.close();
		return temp;
	}

}