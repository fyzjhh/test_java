package com.test.lucene;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;

/**
 * 搜索索引 Lucene 3.0+
 * 
 * @author Administrator
 * 
 */
public class Searcher {

	public static void main(String[] args) throws IOException, ParseException {
		// 保存索引文件的地方
		String indexDir = "E:\\temp\\li";
		Directory dir = new SimpleFSDirectory(new File(indexDir));
		// 创建 IndexSearcher对象，相比IndexWriter对象，这个参数就要提供一个索引的目录就行了
		IndexSearcher indexSearch = new IndexSearcher(dir);

		
		Query query = new TermQuery(new Term("body", "Students should be allowed to go out with their friends, but not allowed to drink beer."));
		TopDocs hits = indexSearch.search(query, null, 100);

		// hits.totalHits表示一共搜到多少个
		System.out.println("找到了" + hits.totalHits + "个");
		// 循环hits.scoreDocs数据，并使用indexSearch.doc方法把Document还原，再拿出对应的字段的值
		for (int i = 0; i < hits.scoreDocs.length; i++) {
			ScoreDoc sdoc = hits.scoreDocs[i];
			Document doc = indexSearch.doc(sdoc.doc);
			System.out.println(doc.get("body"));
		}
		indexSearch.close();
	}
}