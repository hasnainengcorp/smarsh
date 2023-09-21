package com.sender.sender.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

@Service
public class SQSService {

	@Value("${aws.queue}")
	private String queueName;

	@Value("${aws.src.bucket}")
	private String srcbucketName;
	
	

	public void sendMessage(String message) {

		// System.out.println("queue name is " + queueName);

		SqsClient sqsClient = SqsClient.builder()

				.region(Region.US_EAST_1)

				.credentialsProvider(ProfileCredentialsProvider.create())

				.build();

		// System.out.println("queue name is " + queueName);

		// System.out.println("queue name is"+queueName);

		System.out.println("message is " + message);

		try {

			CreateQueueRequest request = CreateQueueRequest.builder().queueName(queueName).build();

			sqsClient.createQueue(request);

			GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder().queueName(queueName).build();

			String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();

			SendMessageRequest sendMsgRequest = SendMessageRequest.builder()

					.queueUrl(queueUrl).

					messageBody(message)

					// .delaySeconds(5)

					.build();

			int i = 0;

			while (i < 51) {

				i++;

				SendMessageResponse res = sqsClient.sendMessage(sendMsgRequest);

				System.out.println(res);

			}

		} catch (SqsException e) {

			System.err.println(e.awsErrorDetails().errorMessage());

			System.exit(1);

		}

	}

	public void receiveMessages() {

		SqsClient sqsClient = SqsClient.builder()

				.region(Region.US_EAST_1)

				.credentialsProvider(ProfileCredentialsProvider.create())

				.build();
		//added s3
		
		   ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
		    Region region = Region.US_EAST_1;
		    
		
		S3Client s3client = S3Client.builder().region(region)
	            .credentialsProvider(credentialsProvider)
	            .build();

		//end of s3

		GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder().queueName(queueName).build();

		String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();

		System.out.println("\nReceive messages");

		try {

			// snippet-start:[sqs.java2.sqs_example.retrieve_messages]

			ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()

					.queueUrl(queueUrl)

					.visibilityTimeout(20)

					.maxNumberOfMessages(5)

					.build();

			List<Message> list = sqsClient.receiveMessage(receiveMessageRequest).messages();
			
			if(list.isEmpty()) {
				System.out.println("no items in the sqs to be process ");
			}

			for (Message message : list) {

				System.out.println("message body is "+message.body());
				System.out.println(message.messageAttributes().values());
				System.out.println(message.attributes());
				System.out.println(message.hasAttributes());
				System.out.println(message.receiptHandle());
				System.out.println(message.messageAttributes());
		
				//System.out.println(message.body()[0]);
						
				
				//parse object
				//S3 Object Key: 
				
				
				
				
				//
				

				// custom logic
				  GetObjectRequest getObjectRequest = GetObjectRequest.builder()
			                .bucket(srcbucketName)
			                .key(message.body())
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

			                   
			                    //String newKey = message.body() + entry.getName();
			                    String newKey = entry.getName();
			                    
			                    System.out.println(newKey);

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
			        
				
				//end of custom logic

			//	String fname = "zipped_files_1692228026018.zip";

				//s3service.getObjectBytes();
				
				
				

				DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()

						.queueUrl(queueUrl)

						.receiptHandle(message.receiptHandle())

						.build();

				sqsClient.deleteMessage(deleteMessageRequest);

			}

			// deleteMessages(sqsClient, queueUrl, list);

			for (Message message : list) {

				System.out.println(message.body());

			}

			for (Message message : list) {

				System.out.println(message.body());

			}

			// return sqsClient.receiveMessage(receiveMessageRequest).messages();

		} catch (SqsException e) {

			System.err.println(e.awsErrorDetails().errorMessage());

			System.exit(1);

		}

		// return null;

		// snippet-end:[sqs.java2.sqs_example.retrieve_messages]

	}
	
	//
	public void sendObjectsInS3ToSQS() {
	    try {
	        // Initialize the S3 client
	        S3Client s3Client = S3Client.builder()
	                .region(Region.US_EAST_1) // Replace with your desired region
	                .credentialsProvider(DefaultCredentialsProvider.create())
	                .build();

	        // Initialize the SQS client
	        SqsClient sqsClient = SqsClient.builder()
	                .region(Region.US_EAST_1) // Replace with your desired region
	                .credentialsProvider(DefaultCredentialsProvider.create())
	                .build();

	        // Get a list of objects in the S3 bucket
	        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
	                .bucket(srcbucketName)
	                .build();

	        ListObjectsV2Response objectListing = s3Client.listObjectsV2(listObjectsRequest);

	        for (S3Object s3Object : objectListing.contents()) {
	            // Create a message with S3 object information and send it to the SQS queue
	        	//send msg to sqs in different format
	        	
	        	/*
	            String messageBody = "S3 Object Key: " + s3Object.key() +
	                    ", Size: " + s3Object.size() +
	                    ", Last Modified: " + s3Object.lastModified();
	                    */
	            String messageBody=s3Object.key();
	            
	            System.out.println(messageBody);

	            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
	                    .queueUrl(getQueueUrl(sqsClient, queueName))
	                    .messageBody(messageBody)
	                    .build();

	            sqsClient.sendMessage(sendMessageRequest);
	        }
	    } catch (S3Exception | SqsException e) {
	        e.printStackTrace();
	    }
	}

	private String getQueueUrl(SqsClient sqsClient, String queueName) {
	    GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
	            .queueName(queueName)
	            .build();

	    return sqsClient.getQueueUrl(getQueueRequest).queueUrl();
	}
	
	//

	public static void deleteMessages(SqsClient sqsClient, String queueUrl, List<Message> messages) {

		System.out.println("\nDelete Messages");

		// snippet-start:[sqs.java2.sqs_example.delete_message]

		try {

			for (Message message : messages) {

				DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()

						.queueUrl(queueUrl)

						.receiptHandle(message.receiptHandle())

						.build();

				sqsClient.deleteMessage(deleteMessageRequest);

			}

			// snippet-end:[sqs.java2.sqs_example.delete_message]

		} catch (SqsException e) {

			System.err.println(e.awsErrorDetails().errorMessage());

			System.exit(1);

		}

	}

	public String s3toSQS() {

		S3Client s3Client = S3Client.builder()

				.region(Region.US_EAST_1)

				.credentialsProvider(ProfileCredentialsProvider.create())

				.build();

		SqsClient sqsClient = SqsClient.builder()

				.region(Region.US_EAST_1)

				.credentialsProvider(ProfileCredentialsProvider.create())

				.build();

		try {

			// Fetch the PDF content from S3

			ResponseBytes responseBytes = s3Client.getObjectAsBytes(GetObjectRequest.builder()

					.bucket(srcbucketName)

					.key("zipped_files_1692228026018.zip")

					.build());

			String pdfContent = new String(responseBytes.asByteArray(), StandardCharsets.US_ASCII);

			// Create the SQS queue if it doesn't exist

			CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()

					.queueName(queueName)

					.build();

			sqsClient.createQueue(createQueueRequest);

			// Get the URL of the SQS queue

			GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()

					.queueName(queueName)

					.build();

			String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();

			// Send the PDF content as a message

			SendMessageRequest sendMessageRequest = SendMessageRequest.builder()

					.queueUrl(queueUrl)

					.messageBody(pdfContent)

					.delaySeconds(5)

					.build();

			sqsClient.sendMessage(sendMessageRequest);

			System.out.println("PDF content sent to SQS queue: " + queueName);

		} catch (S3Exception | SqsException e) {

			System.err.println("Error: " + e.getMessage());

			e.printStackTrace();

		} finally {

			s3Client.close();

			sqsClient.close();

			return "file uploaed ";

		}

	}


	
}