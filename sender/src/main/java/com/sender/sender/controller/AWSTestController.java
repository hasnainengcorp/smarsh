package com.sender.sender.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.sender.sender.AWSTestService;
import com.sender.sender.model.S3Message;
import com.sender.sender.model.SQSMessage;

@RestController
@RequestMapping("/awstest")
public class AWSTestController {

    @Autowired
    private AWSTestService service;

    @PostMapping(value="/message")
    public ResponseEntity<Void> sendMessage(@RequestBody SQSMessage sqsMessage){
    	
        System.out.println("from postman "+sqsMessage.getMsg());
        service.sendMessage(sqsMessage);
 
        return ResponseEntity.ok().build();
    }
    

    @GetMapping(value="/s3data")
    public void getData(){
     
    	System.out.println("get data from s3bucket");
        service.gets3Data();       
    }
    
    //send s3 bucket name to sqs queue
    
    @GetMapping(value = "/s3tosqs")
    public void senddatas3tosqs() {
    	 service.senddatas3tosqs();
    }
    
    //get message from sqs and extract file in s3
    
    @GetMapping(value="/getmessage")
    public void getMessage() {
    	service.receiveMessages();
    }
    
    
    @GetMapping(value = "/s3extract")
    public void extract() {
    	service.extracts3data();
    }
    
    
    @PostMapping(value = "/folder")
    public void createFldr(@RequestParam String fname) {
    	System.out.println(fname);
    	service.createFolder(fname);
    }
    
    
    
    
    @PostMapping(value = "/copys3data")
    public String copyData(@RequestBody S3Message filename) {
   System.out.println(filename.getMsg());
    	return service.copyData("xml-file-2.xml");
    	
    }
    
   
    

}