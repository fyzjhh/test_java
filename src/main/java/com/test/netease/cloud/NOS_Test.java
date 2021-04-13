package com.test.cloud;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.netease.cloud.ClientException;
import com.netease.cloud.ServiceException;
import com.netease.cloud.auth.PropertiesCredentials;
import com.netease.cloud.services.nos.NosClient;
import com.netease.cloud.services.nos.model.CompleteMultipartUploadRequest;
import com.netease.cloud.services.nos.model.CompleteMultipartUploadResult;
import com.netease.cloud.services.nos.model.InitiateMultipartUploadRequest;
import com.netease.cloud.services.nos.model.InitiateMultipartUploadResult;
import com.netease.cloud.services.nos.model.ListPartsRequest;
import com.netease.cloud.services.nos.model.PartETag;
import com.netease.cloud.services.nos.model.PartListing;
import com.netease.cloud.services.nos.model.PartSummary;
import com.netease.cloud.services.nos.model.PutObjectRequest;
import com.netease.cloud.services.nos.model.UploadPartRequest;
import com.netease.cloud.services.nos.model.UploadPartResult;

public class NOS_Test {

	public static void main(String[] args) throws Exception {

		if (args.length == 3) {
			testMultiUpload(args[0], args[1], args[2]);
			// testUpload("ddb-offline-bak", "club-archive-ddb1", //
			// "D:/temp/user.sql");
		} else {
			System.out.println("args count is not 3");
		}

		// testMultiUpload("ddb-offline-bak", "multi1", "D:/temp/core.10980");
		System.out.println("====success====");
	}

	private static void testUpload(String bn, String key, String filestr)
			throws Exception {

		NosClient nosClient = new NosClient(new PropertiesCredentials(
				NOS_Test.class.getResourceAsStream("credentials.properties")));
		nosClient.setEndpoint("114.113.202.21");
		String bucketName = "ddb-offline-bak";

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

	public static void testMultiUpload(String bn, String key, String testFile)
			throws Exception {
		NosClient cli = new NosClient(new PropertiesCredentials(
				NOS_Test.class.getResourceAsStream("credentials.properties")));
		cli.setEndpoint("114.113.202.21");

		InitiateMultipartUploadRequest imur = new InitiateMultipartUploadRequest(
				bn, key);
		InitiateMultipartUploadResult res = cli.initiateMultipartUpload(imur);
		String upId = res.getUploadId();

		FileInputStream fis = new FileInputStream(testFile);
		long fileLen = new File(testFile).length();
		int sliceNum = (int) (fileLen / (50 * 1024 * 1024)); // ·Ö¿éÊý
		if (sliceNum == 0){
			sliceNum = 1;
		}
		int averageSize = (int) (fileLen / sliceNum);
		int partsNum = (fileLen % sliceNum) == 0 ? sliceNum : (sliceNum + 1);
		// ·Ö¿é
		long offset = 0;
		int i = 0;
		int rbcnt;
		byte[] buffer = new byte[averageSize];

		while ((i < partsNum) && (rbcnt = fis.read(buffer)) != -1) {

			InputStream iss = new ByteArrayInputStream(buffer, 0, rbcnt);
			UploadPartRequest request = new UploadPartRequest();
			request.withBucketName(bn).withInputStream(iss).withKey(key)
					.withUploadId(upId).withPartNumber(i + 1)
					.withPartSize(rbcnt);
			UploadPartResult uploadPartResult = cli.uploadPart(request);
			String actualMD5 = TestHelper.getMD5(request.getInputStream());
			String tmpEtag = uploadPartResult.getETag();
			String tmpActual = actualMD5;
			if (tmpEtag.equalsIgnoreCase(tmpActual)) {
				System.out.println("upload successful part " + i);
			} else {
				System.out.println("upload failed part " + i);
			}
			offset += rbcnt;
			i++;
		}
		fis.close();

		ListPartsRequest listPartsRequest = new ListPartsRequest(bn, key, upId);
		PartListing parts = cli.listParts(listPartsRequest);
		List<PartETag> partETags = new ArrayList<PartETag>();
		for (PartSummary par : parts.getParts()) {
			partETags.add(new PartETag(par.getPartNumber(), par.getETag()));
		}
		CompleteMultipartUploadRequest cmur = new CompleteMultipartUploadRequest(
				bn, key, upId, partETags);
		cli.completeMultipartUpload(cmur);
	}

}
