package com.sender.sender.service;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
//import com.amazonaws.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

@Service
public class S3Service {

	@Value("${aws.src.bucket}")
	private String srcbucketName;
	
	@Value("${aws.dst.bucket}")
	private String dstbucketName;

	public void getObjectBytes() {
		processs3dataextract();
		System.out.println("in service of s3 ");

		ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
		Region region = Region.US_EAST_1;

		S3Client s3client = S3Client.builder().region(region)
				.credentialsProvider(credentialsProvider)
				.build();

		
		
		ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(srcbucketName).build();

		ListObjectsV2Response listObjectsV2Response = s3client.listObjectsV2(listObjectsV2Request);

		List<S3Object> contents = listObjectsV2Response.contents();

		System.out.println("Number of objects in the bucket: " + contents.stream().count());
		contents.stream().forEach(System.out::println);

		for (S3Object object : contents) {
			//downloads3Object(object.key());
			String key = object.key();
			
			System.out.println(key);
			System.out.println(object.size());
			//System.out.println(object.toString());

		}
		
		

		s3client.close();
		
	//-----alternative way---
		
//		  String bucketName = srcbucketName;
//
//	        S3Client s3 = S3Client.builder()
//	                .region(Region.US_EAST_1) // Change the region if your bucket is in a different region
//	                .build();
//
//	        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
//	                .bucket(bucketName)
//	                .build();
//
//	        ListObjectsV2Response listObjectsResponse = s3.listObjectsV2(listObjectsRequest);
//
//	        for (S3Object content : listObjectsResponse.contents()) {
//	            System.out.println("Object Key: " + content.key());
//	        }
	}

/*
	public void extracts3Data() {
	    String bucketName = srcbucketName;

	    S3Client s3 = S3Client.builder()
	            .region(Region.US_EAST_1) // Change the region if your bucket is in a different region
	            .build();

	    ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
	            .bucket(bucketName)
	            .build();

	    ListObjectsV2Response listObjectsResponse = s3.listObjectsV2(listObjectsRequest);

	    for (S3Object content : listObjectsResponse.contents()) {
	        System.out.println("Object Key: " + content.key());

	        if (content.key().equals("iszipped_files_1692228026018.zip")) {
	            // Define a temporary directory to extract files
	            Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"), "s3_unzip_temp");
	            try {
	                Files.createDirectories(tempDir);
	            } catch (IOException e) {
	                e.printStackTrace();
	                return;
	            }

	            // Step 2: Unzip the object
	            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
	                    .bucket(bucketName)
	                    .key(content.key())
	                    .build();
	            S3Object s3Object = s3.getObject(getObjectRequest);
	            
	            try (ZipInputStream zipInputStream = new ZipInputStream(s3Object.res().asInputStream())) {
	                ZipEntry entry;
	                while ((entry = zipInputStream.getNextEntry()) != null) {
	                    Path entryFile = tempDir.resolve(entry.getName());
	                    Files.createDirectories(entryFile.getParent());
	                    Files.copy(zipInputStream, entryFile, StandardCopyOption.REPLACE_EXISTING);
	                }
	            } catch (IOException e) {
	                e.printStackTrace();
	            }

	            // Step 3: Upload unzipped files back to S3
	            try (DirectoryStream<Path> stream = Files.newDirectoryStream(tempDir)) {
	                for (Path entry : stream) {
	                    if (Files.isRegularFile(entry)) {
	                        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
	                                .bucket(bucketName)
	                                .key("unzipped/" + entry.getFileName().toString())
	                                .build();
	                        s3.putObject(putObjectRequest, RequestBody.fromFile(entry));
	                    }
	                }
	            } catch (IOException e) {
	                e.printStackTrace();
	            }

	            // Clean up temporary directory
	            try {
	                Files.walkFileTree(tempDir, new SimpleFileVisitor<Path>() {
	                    @Override
	                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
	                        Files.delete(file);
	                        return FileVisitResult.CONTINUE;
	                    }

	                    @Override
	                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
	                        Files.delete(dir);
	                        return FileVisitResult.CONTINUE;
	                    }
	                });
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
	        }
	    }
	}
*/

	public void downloads3Object(String keyname) {
		System.out.println("file getting downlaod");
		System.out.println(keyname);
		ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
		Region region = Region.US_EAST_1;

		S3Client s3client = S3Client.builder().region(region)
				.credentialsProvider(credentialsProvider)
				.build();
		
		GetObjectRequest request = GetObjectRequest.builder()
		.bucket(srcbucketName)
		.key(keyname)
		.build();
		
		try {
		ResponseInputStream<GetObjectResponse> response = s3client.getObject(request);
		
		  String localFilePath = "C:\\Users\\HasnainAhmed\\Desktop\\s3\\" + keyname;

		    BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(localFilePath));
		
		
	 byte[] buffer = new byte[4096];
		int bytesRead = -1;
	
	while ((bytesRead = response.read(buffer)) !=-1) {
		outputStream.write(buffer, 0, bytesRead);
		}
	
		response.close();
		outputStream.close();
		uploadfiles3();
		
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
	
	public void uploadfiles3() {
		
	        String fileName = "sample_CustomersOrders.xml";
	        String filePath = "C:\\Users\\HasnainAhmed\\Desktop\\s3\\" + fileName;
	        
	    	ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
			Region region = Region.US_EAST_1;
	         
	        S3Client s3client = S3Client.builder().region(region)
					.credentialsProvider(credentialsProvider)
					.build();
	         
	        PutObjectRequest request = PutObjectRequest.builder()
	                            .bucket(dstbucketName).key(fileName).build();
	         
	        s3client.putObject(request, RequestBody.fromFile(new File(filePath)));
	                 
	    }
	
	//public  String copyBucketObject (S3Client s3, String fromBucket, String objectKey, String toBucket) {	
	
	public  String copyBucketObject (String objectKey) {
		System.out.println("data that is to be copid is "+objectKey);
		ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
		Region region = Region.US_EAST_1;
         
        S3Client s3client = S3Client.builder().region(region)
				.credentialsProvider(credentialsProvider)
				.build();
        
        String fromBucket=srcbucketName;
       String  toBucket=dstbucketName;
        System.out.println("source bucket is "+srcbucketName);
        System.out.println("destination bucket is "+dstbucketName);
        
        
				

        CopyObjectRequest copyReq = CopyObjectRequest.builder()
            .sourceBucket(fromBucket)
            .sourceKey(objectKey)
            .destinationBucket(toBucket)
            .destinationKey(objectKey)
            .build();

        
        try {
            CopyObjectResponse copyRes = s3client.copyObject(copyReq);
            return copyRes.copyObjectResult().toString();

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "data uploaded";
    }
	
	//------------------
	public void processs3dataextract() {

	    System.out.println("in service of s3 ");

	    ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
	    Region region = Region.US_EAST_1;

	    S3Client s3client = S3Client.builder().region(region)
	            .credentialsProvider(credentialsProvider)
	            .build();

	    ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(srcbucketName).build();
	    ListObjectsV2Response listObjectsV2Response = s3client.listObjectsV2(listObjectsV2Request);

	    List<S3Object> contents = listObjectsV2Response.contents();

	    System.out.println("Number of objects in the bucket: " + contents.size());

	    for (S3Object object : contents) {
	        String key = object.key();
	        System.out.println(key);
	        System.out.println(object.size());

	        // Fetch the object
	        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
	                .bucket(srcbucketName)
	                .key(key)
	                .build();

	        
	        ResponseBytes<GetObjectResponse> objectBytes = s3client.getObjectAsBytes(getObjectRequest);
	        byte[] data = objectBytes.asByteArray();

	        // Unzip the data
	        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
	             ZipInputStream zis = new ZipInputStream(bais)) {

	            ZipEntry entry;
	            while ((entry = zis.getNextEntry()) != null) {
	                byte[] buffer = new byte[1024];
	                int len;
	                ByteArrayOutputStream baos = new ByteArrayOutputStream();

	                while ((len = zis.read(buffer)) > 0) {
	                    baos.write(buffer, 0, len);
	                }
	                //
	                
	                String newKey = "zipped_files_1692228026018/" + entry.getName();
	                // Upload the unzipped file to S3
	              //  createFolder("zipped_files_1692228026018.zip/");
	                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
	                        .bucket(srcbucketName)
	                        .key(newKey)  // Use entry.getName() as the new key
	                        .build();

	                ByteArrayInputStream unzippedData = new ByteArrayInputStream(baos.toByteArray());
	                s3client.putObject(putObjectRequest, RequestBody.fromInputStream(unzippedData, unzippedData.available()));
	            }
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }

	    s3client.close();
	}
	//--------------
	
	public void createFolder(String foldername) {
		/*
		System.out.println("the folder to be created is"+foldername);
	         
		System.out.println("in service of s3 ");

	    S3Client client = S3Client.builder().build();
        
        PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(srcbucketName).key(foldername).build();
         
        client.putObject(request, RequestBody.empty());
         
        System.out.println("Folder " + foldername + " is ready."); 
        
         */
		
		//, the above program terminates immediately, regardless of the folder has been created or not.
		//In case you want to run some logics that depend on the existence of the folder, you use a S3Waiter object 
		
		S3Client client = S3Client.builder().build();
        
        PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(srcbucketName).key(foldername).build();
         
         
        client.putObject(request, RequestBody.empty());
         
        S3Waiter waiter = client.waiter();
        HeadObjectRequest requestWait = HeadObjectRequest.builder()
                        .bucket(srcbucketName).key(foldername).build();
         
        WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);
         
        waiterResponse.matched().response().ifPresent(System.out::println);
         
        System.out.println("Folder " + foldername + " is ready.");  

	}
	
	
	//reads s3 data and if it is zip file then extract it n keep it in s3
	public void dummyprocesss3dataextract() {
	    System.out.println("in service of s3 ");

	    ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
	    Region region = Region.US_EAST_1;

	    S3Client s3client = S3Client.builder().region(region)
	            .credentialsProvider(credentialsProvider)
	            .build();

	    ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(srcbucketName).build();
	    ListObjectsV2Response listObjectsV2Response = s3client.listObjectsV2(listObjectsV2Request);

	    List<S3Object> contents = listObjectsV2Response.contents();

	    System.out.println("Number of objects in the bucket: " + contents.size());

	    for (S3Object object : contents) {
	        String key = object.key();
	        System.out.println(key);
	        System.out.println(object.size());

	        // Fetch the object
	        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
	                .bucket(srcbucketName)
	                .key(key)
	                .build();

	        ResponseBytes<GetObjectResponse> objectBytes = s3client.getObjectAsBytes(getObjectRequest);
	        byte[] data = objectBytes.asByteArray();

	        if (isZipFile(data)) {
	            // Unzip the data
	            try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
	                 ZipInputStream zis = new ZipInputStream(bais)) {

	                ZipEntry entry;
	                while ((entry = zis.getNextEntry()) != null) {
	                    byte[] buffer = new byte[1024];
	                    int len;
	                    ByteArrayOutputStream baos = new ByteArrayOutputStream();

	                    while ((len = zis.read(buffer)) > 0) {
	                        baos.write(buffer, 0, len);
	                    }

	                    String newKey = object.key() + entry.getName();

	                    // Upload the unzipped file to S3
	                    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
	                            .bucket(srcbucketName)
	                            .key(newKey)
	                            .build();

	                    ByteArrayInputStream unzippedData = new ByteArrayInputStream(baos.toByteArray());
	                    s3client.putObject(putObjectRequest, RequestBody.fromInputStream(unzippedData, unzippedData.available()));
	                }
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
	        }
	    }

	    s3client.close();
	}

	private boolean isZipFile(byte[] data) {
	    if (data.length >= 4) {
	        return data[0] == 0x50 && data[1] == 0x4B && data[2] == 0x03 && data[3] == 0x04;
	    }
	    return false;
	}
	
	
	

}


