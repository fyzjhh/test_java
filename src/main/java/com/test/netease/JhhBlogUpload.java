package com.test.netease.other;

import java.awt.AWTException;
import java.awt.Robot;
import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.awt.event.KeyEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.support.ui.Select;

public class JhhBlogUpload {

	private static final String IB = "   insert into Blog                                                        "
			+ "   (                                                                       "
			+ "   ID ,UserID ,PublishTime ,ModifyTime ,Title ,                            "
			+ "   Abstract ,Content ,IsPublished ,Permalink ,TrackbackUrl ,               "
			+ "   CommentCount ,TrackbackCount ,AllowView ,AllowComment ,ClassID ,        "
			+ "   ClassName ,Tag ,AbstractSysGen ,UserName ,UserNickname ,                "
			+ "   Valid ,AccessCount ,IsBlogAbstractComplete ,MoveFrom ,circleIds ,       "
			+ "   Popularity ,IP ,Compressed ,ZipContent ,Rank                            "
			+ "   )\n                                                                       "
			+ "   values                                                                  "
			+ "   \n(                                                                       "
			+ "   seq,?,?,NULL,?,                                                         "
			+ "   NULL,?,1,?,?,                                                           "
			+ "   NULL,0,-100,-100,?,                                                     "
			+ "   ?,NULL,1,'panhaojhh','panhaojhh',                                       "
			+ "   0,0,0,NULL,NULL,                                                        "
			+ "   0,'114.113.13.56',0,NULL,10                                            "
			+ "   )                                                                       ";
	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	static long macrosecsinday = 24 * 3600 * 1000;
	static WebDriver driver = null;
	static Connection conn = null;
	static String url = "10.100.83.153:8888?key=D:/NetEase/ddb/ddb3/conf/secret.key&logdir=log/log5";

	public static void main(String[] args) throws Exception {
		launch();
		uploadBlogs("D:\\temp\\jbc\\1632004");

		// uploadBlog("D:/temp/jbc/byclass/");
	}

	public static void openConn() throws Exception {

		Class.forName("com.netease.backend.db.DBDriver");
		conn = DriverManager.getConnection(url, "jhh", "jhh");

	}

	public static void closeConn() throws Exception {
		conn.close();
	}

	private static void uploadBlog(String fold) throws Exception {

		openConn();

		File basefs = new File(fold);

		File[] fs1 = basefs.listFiles();

		for (int m = 0; m < fs1.length; ++m) {
			String dn1 = fs1[m].getName();
			System.out.println("==== add catelog " + dn1);
			File f2 = fs1[m];

			File[] fs2 = f2.listFiles();
			for (int n = 0; n < fs2.length; ++n) {
				String dn2 = fs2[n].getName();

				long ts = System.currentTimeMillis();

				long UserID = 208276197;
				long PublishTime = ts;
				String Title = dn2.replace(".blog", "");
				String Content = readFileByLines(fs2[n].getAbsolutePath());

				String linkurl = (ts + "00" + ts).substring(0, 24);
				String Permalink = "panhaojhh/blog/static/" + linkurl;
				String TrackbackUrl = "panhaojhh/blog/" + linkurl + ".track";

				long ClassID = 252928207;
				String ClassName = "java";

				PreparedStatement stinblog = conn.prepareStatement(IB);
				stinblog.setLong(1, UserID);
				stinblog.setLong(2, PublishTime);
				stinblog.setString(3, Title);
				stinblog.setString(4, Content);
				stinblog.setString(5, Permalink);
				stinblog.setString(6, TrackbackUrl);
				stinblog.setLong(7, ClassID);
				stinblog.setString(8, ClassName);

				int rs = stinblog.executeUpdate();
				if (rs == 1) {
					System.out.println("====seccess sql: " + Title + "\n\n");
					Thread.sleep(1000 * 3);
				}

			}

		}

		closeConn();

	}

	public static String quotaStr(String s) {
		return "'" + s + "'";
	}

	public static String readFileByLines(String fileName) {
		File file = new File(fileName);
		BufferedReader reader = null;
		String fs = "";
		try {

			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 0;
			// һ�ζ���һ�У�ֱ������nullΪ�ļ�����
			while ((tempString = reader.readLine()) != null) {
				// ��ʾ�к�
				fs += tempString + "\n";
				line++;
			}
			reader.close();

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
		return fs;
	}

	private static void uploadBlogs(String fold) throws Exception {

		File basefs = new File(fold);

		File[] fs1 = basefs.listFiles();

		for (int m = 0; m < fs1.length; ++m) {
			String dn1 = fs1[m].getName();
//			addCatelog(dn1);
			System.out.println("==== add catelog " + dn1);
			File f2 = fs1[m];

			File[] fs2 = f2.listFiles();
			for (int n = 0; n < fs2.length; ++n) {
				String dn2 = fs2[n].getName();
				System.out.println("==== begin upload file :" + dn2);
				uploadOneBlog(fs2[n]);
				System.out.println("==== success upload blog :" + dn2);
				Thread.sleep(1000 * 3);
			}

		}

	}

	private static void uploadOneBlog(File fdf) throws FileNotFoundException,
			IOException, InterruptedException {
		BufferedReader rf = null;
		String tls = null;
		String btitle = "";
		String bc = "";
		rf = new BufferedReader(new FileReader(fdf));
		int line = 0;
		while ((tls = rf.readLine()) != null) {
			if (line == 0) {
				btitle = tls;
			} else {
				bc += tls + "\n";
			}
			line++;
		}
		rf.close();

		driver.findElement(
				By.xpath("//a[@class='i i1 fc01 h' and (text()='��־')]"))
				.click();
		driver.findElement(
				By.xpath("//a[@class='wb nbtn bdc1 bgc2 fc09 fs1 fw1 ztag' and (text()='д��־')]"))
				.click();

		driver.findElement(
				By.xpath("//input[@class='ztag' and @type='text' and @value='�������������']"))
				.sendKeys(btitle);
		driver.findElement(
				By.xpath("//div[@class='zicn z-icn-150' and (text()='Դ����')]"))
				.click();

		// driver.findElement(
		// By.xpath("//div[@id='-2']/div[2]/div/form/div/div/div/div/div[4]/div/div/textarea"))
		// .sendKeys(bcontent);

		driver.findElement(
				By.xpath("//div[@id='-2']/div[2]/div/form/div/div/div/div/div[4]/div/div/textarea"))
				.click();
		Clipboard clipboard = Toolkit.getDefaultToolkit().getSystemClipboard();
		StringSelection tText = new StringSelection(bc);
		clipboard.setContents(tText, null);

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Robot rb = null;
		try {
			rb = new Robot();
		} catch (AWTException e) {
			e.printStackTrace();
		}

		rb.keyPress(KeyEvent.VK_CONTROL); // ���°���
		rb.keyPress(KeyEvent.VK_V); // �ͷŰ���
		rb.keyRelease(KeyEvent.VK_V); // �ͷŰ���
		rb.keyRelease(KeyEvent.VK_CONTROL); // ���°���

		Select cateSel = new Select(
				driver.findElement(By
						.xpath("//select[@id='key-093402170-0' and @class='bdwa bds0 bdc0 ytag']")));

		cateSel.selectByIndex(0);

		driver.findElement(
				By.xpath("//input[@class='nbtn bdc1 bgc2 fc09' and @type='button' and @value='������־']"))
				.click();

	}

	private static void launch() {
		// ��chrome
		System.setProperty("webdriver.chrome.driver",
				"E:\\Study\\selenium\\chromedriver.exe");
		System.setProperty(
				"webdriver.chrome.bin",
				"C:\\Users\\Administrator\\AppData\\Local\\Google\\Chrome\\Application\\chrome.exe");
		driver = new ChromeDriver();

		driver.get("http://panhaojhh.blog.163.com");

		driver.findElement(
				By.xpath("//a[@rel='nofollow' and @class='iblock fr ima ztag' and (text()='��¼')]"))
				.click();

		driver.findElement(
				By.xpath("//input[@class='zcom ztxt' and @type='text']"))
				.sendKeys("panhaojhh@163.com");
		driver.findElement(By.xpath("//input[@name='password']")).sendKeys(
				"panhaojhh87");
		driver.findElement(
				By.xpath("//input[@class='wbtn wbtnok' and @type='button'and @value='��  ¼']"))
				.click();

	}

	private static void addCatelog(String catelog) {

		driver.findElement(
				By.xpath("//a[@class='i i1 fc01 h' and (text()='��־')]"))
				.click();
		driver.findElement(By.xpath("//span[@class='ul' and (text()='�������')]"))
				.click();
		driver.findElement(
				By.xpath("//span[@class='ztag ul fc04' and (text()='�½�����')]"))
				.click();
		driver.findElement(
				By.xpath("//input[@class='ztag bdc0' and @type='text']"))
				.sendKeys(catelog);
		driver.findElement(
				By.xpath("//input[@class='ztag nbw-win wbtnok' and @type='button' and @value='���']"))
				.click();
		driver.findElement(
				By.xpath("//input[@class='nbwinbtn sep wbtn wbtnok' and @type='button' and @value='ȷ��']"))
				.click();
	}
}