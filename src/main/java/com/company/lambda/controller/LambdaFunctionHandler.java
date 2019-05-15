package com.company.lambda.controller;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    private AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    private DynamoDB dynamoDB = new DynamoDB(client);
    private static String tableName = "Company";
    private static Log LOGGER = LogFactory.getLog(LambdaFunctionHandler.class);
    
    public LambdaFunctionHandler() {}

    // Test purpose only.
    LambdaFunctionHandler(AmazonS3 s3) {
        this.s3 = s3;
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
    	LOGGER.info("Received event: " + event);
        String contentType = "";
        // Get the object from the event and show its content type
        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String key = event.getRecords().get(0).getS3().getObject().getKey();
        try {
            S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
            contentType = response.getObjectMetadata().getContentType();
            LOGGER.info("CONTENT TYPE: " + contentType);
            BufferedReader br = new BufferedReader(new InputStreamReader(response.getObjectContent()));
            String csvOutput;
            while ((csvOutput = br.readLine()) != null) {
                String[] str = csvOutput.split(",");
                LOGGER.info("str: " + str);
                int total = 0;
                int average = 0;
                for (int i = 1; i < str.length; i++) {
                    total += Integer.valueOf(str[i]);
                }
                average = total / (str.length - 1);
                LOGGER.info("AVERAGE: " + average);
               
                createDynamoItem(Integer.valueOf(str[0]), average);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error getting object %s from bucket %s. Make sure they exist and"
                + " your bucket is in the same region as this function.", key, bucket));
          }
        return contentType;
    }
    
    private void createDynamoItem(int studentId, int grade) {
    	 
        Table table = dynamoDB.getTable(tableName);
        try {
 
            Item item = new Item().withPrimaryKey("companyID", studentId).withString("Grade", String.valueOf(grade));
            table.putItem(item);
 
        } catch (Exception e) {
            System.err.println("Create item failed.");
            System.err.println(e.getMessage());
 
        }
    }
}