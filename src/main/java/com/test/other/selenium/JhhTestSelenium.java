package com.test.selenium;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

public class JhhTestSelenium {
	private static final String FG = "--";
	static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	static long macrosecsinday = 24 * 3600 * 1000;

	public static void main(String[] args) throws Exception {

		uploadBlog("D:\\temp\\jbc\\lanvis");

	}

	private static void uploadBlog(String fold) throws Exception {

		// 打开chrome
		System.setProperty("webdriver.chrome.driver",
				"E:\\Study\\selenium\\chromedriver.exe");
		System.setProperty(
				"webdriver.chrome.bin",
				"C:\\Users\\Administrator\\AppData\\Local\\Google\\Chrome\\Application\\chrome.exe");
		WebDriver driver = new ChromeDriver();

		driver.get("http://fyzjhh.blog.163.com");

		driver.findElement(
				By.xpath("//a[@rel='nofollow' and @class='iblock fr ima ztag' and (text()='登录')]"))
				.click();

		driver.findElement(
				By.xpath("//input[@class='zcom ztxt' and @type='text']"))
				.sendKeys("panhaojhh@163.com");
		driver.findElement(By.xpath("//input[@name='password']")).sendKeys(
				"panhaojhh87");
		driver.findElement(
				By.xpath("//input[@class='wbtn wbtnok' and @type='button'and @value='登  录']"))
				.click();
		driver.findElement(
				By.xpath("//a[@class='pleft fc03 fw1 ul view viewr' and (text()='查看我的博客')]"))
				.click();

		BufferedReader rf = null;
		String tls = null;
		int i = 0;
		File fdf = new File(fold);
		for (File f : fdf.listFiles()) {

			String bcontent = "";
			if (f.isFile() && f.getName().endsWith(".htm")) {
				String fn = f.getName();

				rf = new BufferedReader(new FileReader(f));
				while ((tls = rf.readLine()) != null) {
					bcontent += tls + "\n";
				}
				rf.close();

				driver.findElement(
						By.xpath("//a[@class='i i1 fc01 h' and (text()='日志')]"))
						.click();
				driver.findElement(
						By.xpath("//a[@class='wb nbtn bdc1 bgc2 fc09 fs1 fw1 ztag' and (text()='写日志')]"))
						.click();

				driver.findElement(
						By.xpath("//input[@class='ztag' and @type='text' and @value='在这里输入标题']"))
						.sendKeys("titlexxxx" + i);
				driver.findElement(
						By.xpath("//div[@class='zicn z-icn-150' and (text()='源代码')]"))
						.click();

				driver.findElement(
						By.xpath("//div[@id='-2']/div[2]/div/form/div/div/div/div/div[4]/div/div/textarea"))
						.sendKeys(bcontent);

				driver.findElement(
						By.xpath("//input[@class='nbtn bdc1 bgc2 fc09' and @type='button' and @value='发表日志']"))
						.click();

				i++;
				System.out.println("==== " + i + " upload blog success:" + fn);
				Thread.currentThread().sleep(1000 * 5);
			}
		}

	}
}