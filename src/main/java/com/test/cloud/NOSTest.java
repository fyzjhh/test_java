package com.test.cloud;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import com.netease.cloud.ClientException;
import com.netease.cloud.ServiceException;
import com.netease.cloud.auth.PropertiesCredentials;
import com.netease.cloud.services.nos.Nos;
import com.netease.cloud.services.nos.NosClient;
import com.netease.cloud.services.nos.model.Bucket;
import com.netease.cloud.services.nos.model.CompleteMultipartUploadRequest;
import com.netease.cloud.services.nos.model.GetObjectRequest;
import com.netease.cloud.services.nos.model.InitiateMultipartUploadRequest;
import com.netease.cloud.services.nos.model.ListObjectsRequest;
import com.netease.cloud.services.nos.model.ListPartsRequest;
import com.netease.cloud.services.nos.model.NOSObject;
import com.netease.cloud.services.nos.model.NOSObjectSummary;
import com.netease.cloud.services.nos.model.ObjectListing;
import com.netease.cloud.services.nos.model.PartETag;
import com.netease.cloud.services.nos.model.PartListing;
import com.netease.cloud.services.nos.model.PartSummary;
import com.netease.cloud.services.nos.model.PutObjectRequest;
import com.netease.cloud.services.nos.model.UploadPartRequest;
import com.netease.cloud.services.nos.transfer.TransferManager;
import com.netease.cloud.services.nos.transfer.Upload;
import com.netease.cloud.services.nos.transfer.model.UploadResult;

public class NOSTest {

	final static String basedir = "E:/MiscDownload/edu/";

	public static void main(String[] args) throws Exception {
		// if (args.length == 3) {
		// // testUpload(args[0],args[1],args[2]);
		//
		// } else {
		// System.out.println("args count is not 3");
		// }
		testUpload("ddb-offline-bak", "club-archive-ddb1",
		"D:/temp/user.txt");
		System.out.println("====success====");
	}

	private static void testUpload(String bn, String key, String filestr)
			throws Exception {

		NosClient nosClient = new NosClient(new PropertiesCredentials(
				NOSTest.class.getResourceAsStream("credentials.properties")));
		nosClient.setEndpoint("114.113.202.21");
		String bucketName = "sa-osimg-bak";

		try {
			System.out.println("uploading file " + filestr);

			File file = new File(filestr);
			PutObjectRequest por = new PutObjectRequest(bucketName, key, file);
			por.setStorageClass("sata-Critical");
			nosClient.putObject(por);

			System.out.println();

		} catch (ServiceException ase) {
			System.out
					.println("ServiceException.some errors occur in server point.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("NOS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (ClientException ace) {
			System.out
					.println("ClientException.some errors occur in client point");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	private static void testDownlod() throws Exception {

		NosClient nosClient = new NosClient(new PropertiesCredentials(
				NOSTest.class.getResourceAsStream("credentials.properties")));
		nosClient.setEndpoint("114.113.202.21");
		String buNm = null;
		String key = null;
		int allNum = 0, buNum = 0;
		try {

			for (Bucket bucket : nosClient.listBuckets()) {
				buNm = bucket.getName();
				System.out.println("========listing objects in bucket " + buNm);
				buNum = 0;
				ListObjectsRequest req = new ListObjectsRequest();
				req.withBucketName(buNm);
				ObjectListing objs = nosClient.listObjects(req);

				for (NOSObjectSummary summ : objs.getObjectSummaries()) {
					key = summ.getKey();

					NOSObject object = nosClient
							.getObject(new GetObjectRequest(buNm, key));
					buNum++;
					if (buNum > 10) {
						break;
					}

					System.out.println("====" + summ.getKey() + "  "
							+ "(size = " + summ.getSize() + ")");

					System.out.println("Content-Type: "
							+ object.getObjectMetadata().getContentType());
					System.out.println("Content-Length: "
							+ object.getObjectMetadata().getContentLength());

					writeFile(object.getObjectContent(), basedir + repfile(key));

				}
				allNum = allNum + buNum;
				if (buNum > 100) {
					break;
				}
			}
			System.out.println();

			System.out.println();

		} catch (ServiceException ase) {
			System.out
					.println("ServiceException.some errors occur in server point.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("NOS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (ClientException ace) {
			System.out
					.println("ClientException.some errors occur in client point");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	private static void testDownlodBucket() throws Exception {

		NosClient nosClient = new NosClient(new PropertiesCredentials(
				NOSTest.class.getResourceAsStream("credentials.properties")));
		nosClient.setEndpoint("114.113.202.21:8181");
		nosClient.deleteObject("edu-video", "宗次郎 - 神雕背景音乐之一故乡的原风景.mp3");
		String buNm = "edu-video";
		String key = null;
		int allNum = 0, buNum = 0;
		// try {
		// System.out.println("========listing objects in bucket " + buNm);
		// buNum = 0;
		// ListObjectsRequest req = new ListObjectsRequest();
		// req.withBucketName(buNm);
		// ObjectListing objs = nosClient.listObjects(req);
		// for (NOSObjectSummary summ : objs.getObjectSummaries()) {
		// try {
		// key = summ.getKey();
		//
		// GetObjectRequest goreq = new GetObjectRequest(buNm, key);
		// NOSObject object = nosClient.getObject(goreq);
		// buNum++;
		// if (buNum > 20) {
		// break;
		// }
		//
		// System.out.println("====" + summ.getKey() + "  "
		// + "(size = " + summ.getSize() + ")");
		//
		// System.out.println("Content-Type: "
		// + object.getObjectMetadata().getContentType());
		// System.out.println("Content-Length: "
		// + object.getObjectMetadata().getContentLength());
		//
		// writeFile(object.getObjectContent(), basedir + repfile(key));
		// } catch (ServiceException ase) {
		// System.out
		// .println("ServiceException.some errors occur in server point.");
		// System.out.println("Error Message:    " + ase.getMessage());
		// System.out.println("HTTP Status Code: "
		// + ase.getStatusCode());
		// System.out.println("NOS Error Code:   "
		// + ase.getErrorCode());
		// System.out.println("Error Type:       "
		// + ase.getErrorType());
		// System.out.println("Request ID:       "
		// + ase.getRequestId());
		// } catch (ClientException ace) {
		// System.out
		// .println("ClientException.some errors occur in client point");
		// System.out.println("Error Message: " + ace.getMessage());
		// }
		// }
		// allNum = allNum + buNum;
		//
		// System.out.println();
		//
		// } catch (ServiceException ase) {
		// System.out
		// .println("ServiceException.some errors occur in server point.");
		// System.out.println("Error Message:    " + ase.getMessage());
		// System.out.println("HTTP Status Code: " + ase.getStatusCode());
		// System.out.println("NOS Error Code:   " + ase.getErrorCode());
		// System.out.println("Error Type:       " + ase.getErrorType());
		// System.out.println("Request ID:       " + ase.getRequestId());
		// } catch (ClientException ace) {
		// System.out
		// .println("ClientException.some errors occur in client point");
		// System.out.println("Error Message: " + ace.getMessage());
		// }
	}

	private static void writeFile(InputStream input, String outf)
			throws Exception {
		FileOutputStream output = new FileOutputStream(outf, false);

		// 一次读64K byteread为一次读入的字节数
		byte[] bytes = new byte[65536];
		int numBytes = 0;

		while ((numBytes = input.read(bytes)) != -1) {
			output.write(bytes, 0, numBytes);
		}

		input.close();
		output.close();
	}

	private static String repfile(String f) {
		if (f != null) {
			return f.replace("\\", "fxg").replace("/", "xg").replace("?", "wh")
					.replace("\t", "tab").replace("<", "xyh")
					.replace(">", "dyh").replace("\"", "syh")
					.replace("|", "sx").replace("*", "xh").replace(":", "mh");
		} else {
			return null;
		}
	}

	public static void ttt(String[] args) throws IOException {
		/*
		 * Be sure fill your access key and secret key in file
		 * 'credentials.properties' .
		 */
		Nos nosClient = new NosClient(new PropertiesCredentials(
				NOSTest.class.getResourceAsStream("credentials.properties")));

		String bucketName = "my-first-bucket";
		String key = "MyObjectKey";

		/** the file size is need large than 11M **/
		String filePathString = "";

		System.out.println("===========================================");
		System.out.println("Getting Started with NOS");
		System.out.println("===========================================\n");

		try {
			/*
			 * If the bucket not existed , then create a new bucket.
			 */
			if (!nosClient.doesBucketExist(bucketName)) {
				System.out.println("Creating bucket " + bucketName + "\n");
				nosClient.createBucket(bucketName);
			}

			/*
			 * List the buckets in your account
			 */
			System.out.println("Listing buckets");
			for (Bucket bucket : nosClient.listBuckets()) {
				System.out.println(" - " + bucket.getName());
			}
			System.out.println();

			/*
			 * Upload an object to your bucket .
			 */
			System.out.println("Uploading a new object to NOS from a file\n");
			nosClient.putObject(new PutObjectRequest(bucketName, key,
					createSampleFile()));

			/*
			 * Download an object - When you download an object, you get all of
			 * the object's metadata and a stream from which to read the
			 * contents.
			 */
			System.out.println("Downloading an object");
			NOSObject object = nosClient.getObject(new GetObjectRequest(
					bucketName, key));
			System.out.println("Content-Type: "
					+ object.getObjectMetadata().getContentType());
			System.out.println("Content-Length: "
					+ object.getObjectMetadata().getContentLength());
			displayTextInputStream(object.getObjectContent());

			/*
			 * List objects in your bucket by prefix - There are many options
			 * for listing the objects in your bucket.
			 */
			System.out.println("Listing objects");
			ObjectListing objectListing = nosClient
					.listObjects(new ListObjectsRequest().withBucketName(
							bucketName).withPrefix("My"));
			for (NOSObjectSummary objectSummary : objectListing
					.getObjectSummaries()) {
				System.out.println(" - " + objectSummary.getKey() + "  "
						+ "(size = " + objectSummary.getSize() + ")");
			}
			System.out.println();

			/*
			 * Upload parts
			 */
			System.out.println("Upload mulit parts\n");
			String mulitKey = "myMultiKey";
			String anotherKey = "anotherKey";
			/** Initiate the upload and get the uploadId. **/
			String uploadId = nosClient.initiateMultipartUpload(
					new InitiateMultipartUploadRequest(bucketName, mulitKey))
					.getUploadId();
			/** Upload the part1 with partNumber=1, size=5M **/
			nosClient.uploadPart(new UploadPartRequest()
					.withBucketName(bucketName).withKey(mulitKey)
					.withUploadId(uploadId).withFile(new File(filePathString))
					// .withInputStream(NosSample.class.getClassLoader().getResourceAsStream(System.getProperty("noSpecial",
					// filePathString)))
					.withPartSize(5 * 1024 * 1024).withPartNumber(1));
			/** Upload the part2 with partNumber=2, size=6M **/
			nosClient.uploadPart(new UploadPartRequest()
					.withBucketName(bucketName).withKey(mulitKey)
					.withUploadId(uploadId).withFile(new File(filePathString))
					// .withInputStream(NosSample.class.getClassLoader().getResourceAsStream(System.getProperty("noSpecial",
					// filePathString)))
					.withPartSize(6 * 1024 * 1024).withPartNumber(2));
			/** List the parts have uploaded. **/
			ListPartsRequest listPartsRequest = new ListPartsRequest(
					bucketName, mulitKey, uploadId);
			PartListing Parts = nosClient.listParts(listPartsRequest);
			List<PartETag> partETags = new ArrayList<PartETag>();
			List<PartSummary> sum = Parts.getParts();
			for (PartSummary part : sum) {
				partETags
						.add(new PartETag(part.getPartNumber(), part.getETag()));
			}
			/** Complete the two parts. **/
			nosClient
					.completeMultipartUpload(new CompleteMultipartUploadRequest(
							bucketName, mulitKey, uploadId, partETags));
			System.out.println("Upload mulit parts finished.");

			/** Another way to use the sdk,upload the file. **/
			TransferManager tx = new TransferManager(new PropertiesCredentials(
					NOSTest.class
							.getResourceAsStream("credentials.properties")));

			/**
			 * This field is not used, but you can use the clientFromManager
			 * like the nosClient.
			 **/
			@SuppressWarnings("unused")
			Nos clientFromManager = tx.getNosClient();

			Upload upload = tx.upload(new PutObjectRequest(bucketName,
					anotherKey, new File(filePathString)));
			try {
				UploadResult result = upload.waitForUploadResult();
				System.out.println(result.getBucketName() + "/"
						+ result.getKey());
			} catch (Exception e) {
				System.out
						.println("Upload failed . Errors occur in uploading as file ,you may be need to upload this file again.");
				System.exit(-1);
			} finally {
				/** Release all resource including threadPool and httpclient. **/
				tx.shutdownNow();
			}

			/*
			 * Delete all objects in your bucket.
			 */
			System.out.println("Deleting an object\n");
			nosClient.deleteObject(bucketName, key);
			nosClient.deleteObject(bucketName, mulitKey);
			nosClient.deleteObject(bucketName, anotherKey);

			/*
			 * Delete a bucket.
			 */
			System.out.println("Deleting bucket " + bucketName + "\n");
			nosClient.deleteBucket(bucketName);

		} catch (ServiceException ase) {
			System.out
					.println("ServiceException.some errors occur in server point.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("NOS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (ClientException ace) {
			System.out
					.println("ClientException.some errors occur in client point");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	/**
	 * Creates a temporary file with text data.
	 * 
	 * @return A newly created temporary file with text data.
	 * 
	 * @throws IOException
	 */
	private static File createSampleFile() throws IOException {
		File file = File.createTempFile("java-sdk-", ".txt");
		file.deleteOnExit();

		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		writer.write("abcdefghijklmnopqrstuvwxyz\n");
		writer.write("01234567890112345678901234\n");
		writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
		writer.write("01234567890112345678901234\n");
		writer.write("abcdefghijklmnopqrstuvwxyz\n");
		writer.close();

		return file;
	}

	/**
	 * Displays the contents of the specified input stream as text.
	 * 
	 * @param input
	 *            The input stream to display as text.
	 * 
	 * @throws IOException
	 */
	private static void displayTextInputStream(InputStream input)
			throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		while (true) {
			String line = reader.readLine();
			if (line == null)
				break;

			System.out.println("    " + line);
		}
		System.out.println();
	}
}
