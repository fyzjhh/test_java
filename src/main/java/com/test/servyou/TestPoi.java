package com.test.servyou;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.poi.xwpf.usermodel.ParagraphAlignment;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFFooter;
import org.apache.poi.xwpf.usermodel.XWPFHeader;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;

public class TestPoi {

	public static void main(String[] args) throws Exception {
		test_word();
	}

	@SuppressWarnings("restriction")
	public static void test_word() throws Exception {
		// Blank Document
		XWPFDocument document = new XWPFDocument();
		// Write the Document in file system
		FileOutputStream out = new FileOutputStream(new File("D:/tmp/1.docx"));
		
		document.getHeaderList();
//        // create header/footer functions insert an empty paragraph
//        XWPFHeader head = document.createHeader(HeaderFooterType.DEFAULT);
//        head.createParagraph().createRun().setText("header");
//        
//        XWPFFooter foot = document.createFooter(HeaderFooterType.DEFAULT);
//        foot.createParagraph().createRun().setText("footer");
        
		// create Paragraph

		String[] title_1_arr = new String[] { "系统基本信息", "系统性能信息", "数据库基本信息", "数据库性能信息", };
		for (int i = 0; i < title_1_arr.length; i++) {
			String title_1 = title_1_arr[i];
			XWPFParagraph paragraph = document.createParagraph();
			paragraph.setStyle("标题 1");
			paragraph.setAlignment(ParagraphAlignment.LEFT);
			XWPFRun run = paragraph.createRun();
			run.setBold(true);
			run.setFontSize(36);
			run.setText(title_1);
			run.addBreak();
		}

		// paragraph.setBorderBottom(Borders.BASIC_BLACK_DASHES);
		// paragraph.setBorderLeft(Borders.BASIC_BLACK_DASHES);
		// paragraph.setBorderRight(Borders.BASIC_BLACK_DASHES);
		// paragraph.setBorderTop(Borders.BASIC_BLACK_DASHES);

		// create table
		XWPFTable table = document.createTable();
		// create first row
		XWPFTableRow tableRowOne = table.getRow(0);
		tableRowOne.getCell(0).setText("col one, row one");
		tableRowOne.addNewTableCell().setText("col two, row one");
		tableRowOne.addNewTableCell().setText("col three, row one");
		// create second row
		XWPFTableRow tableRowTwo = table.createRow();
		tableRowTwo.getCell(0).setText("col one, row two");
		tableRowTwo.getCell(1).setText("col two, row two");
		tableRowTwo.getCell(2).setText("col three, row two");
		// create third row
		XWPFTableRow tableRowThree = table.createRow();
		tableRowThree.getCell(0).setText("col one, row three");
		tableRowThree.getCell(1).setText("col two, row three");
		tableRowThree.getCell(2).setText("col three, row three");

		document.write(out);

//		XWPFWordExtractor we = new XWPFWordExtractor(document);
//		System.out.println(we.getText());
		out.close();

		System.out.println("createparagraph.docx written successfully");

	}


}
