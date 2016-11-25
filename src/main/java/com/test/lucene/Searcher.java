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
 * �������� Lucene 3.0+
 * 
 * @author Administrator
 * 
 */
public class Searcher {

	public static void main(String[] args) throws IOException, ParseException {
		// ���������ļ��ĵط�
		String indexDir = "E:\\temp\\li";
		Directory dir = new SimpleFSDirectory(new File(indexDir));
		// ���� IndexSearcher�������IndexWriter�������������Ҫ�ṩһ��������Ŀ¼������
		IndexSearcher indexSearch = new IndexSearcher(dir);

		
		Query query = new TermQuery(new Term("body", "Students should be allowed to go out with their friends, but not allowed to drink beer."));
		TopDocs hits = indexSearch.search(query, null, 100);

		// hits.totalHits��ʾһ���ѵ����ٸ�
		System.out.println("�ҵ���" + hits.totalHits + "��");
		// ѭ��hits.scoreDocs���ݣ���ʹ��indexSearch.doc������Document��ԭ�����ó���Ӧ���ֶε�ֵ
		for (int i = 0; i < hits.scoreDocs.length; i++) {
			ScoreDoc sdoc = hits.scoreDocs[i];
			Document doc = indexSearch.doc(sdoc.doc);
			System.out.println(doc.get("body"));
		}
		indexSearch.close();
	}
}