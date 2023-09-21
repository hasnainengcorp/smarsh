package com.sender.sender;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.sender.sender.model.SQSMessage;
import com.sender.sender.service.S3Service;
import com.sender.sender.service.SQSService;

@Service
public class AWSTestService {

    @Autowired
    private SQSService sqsService;
    
    @Autowired
    private S3Service s3Service;

    public void sendMessage(SQSMessage sqsMessage) {
        String msg = sqsMessage.getMsg();
        System.out.println("in send message awstestservice "+msg);
        sqsService.sendMessage(msg);
    }
    
    public void receiveMessages() {
     
        System.out.println("in receive message awstestservice ");
        sqsService.receiveMessages();
    }
    
    
    
    public void gets3Data() {
   	 System.out.println("in awstest service  ");
       
        s3Service.getObjectBytes();
    }
    
    
    public void extracts3data() {
    	//s3Service.processs3dataextract();
    	s3Service.dummyprocesss3dataextract();
    }
    
    public void createFolder(String fname) {
    	s3Service.createFolder(fname);
    }
    
    
    public String copyData(String filename) {
    	System.out.println("data copying"+filename);
    	String data=s3Service.copyBucketObject(filename);
    	return data;
    }
    
    public void senddatas3tosqs() {
    	 sqsService.sendObjectsInS3ToSQS();
    }
}