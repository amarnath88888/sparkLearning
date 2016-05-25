package test.testSpark;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

public class Converter {
	
    public static int PRETTY_PRINT_INDENT_FACTOR = 0;
    public String outputDir = "output/";
    
	public static void main(String[] args) {
	        try {
	        	String content = new Scanner(new File("data/input/HealthData/export.xml")).useDelimiter("\\Z").next();
	        	File file = new File("data/Output/healthData.json");
	            JSONObject xmlJSONObj = XML.toJSONObject(content);
	            JSONObject healthDataJSONObj = xmlJSONObj.getJSONObject("HealthData");
	            JSONArray jsonMainArr = healthDataJSONObj.getJSONArray("Record");
	   /*         for (int arrayElement = 0; arrayElement<jsonMainArr.length(); arrayElement++)
	            System.out.println(jsonMainArr.get(arrayElement).toString());*/
	            try {
	            if (!file.exists()) {
					
						file.createNewFile();
					}else{
						file.delete();
					}
				FileWriter fw = new FileWriter(file.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw);
				for (int arrayElement = 0; arrayElement<jsonMainArr.length(); arrayElement++){
				bw.write(jsonMainArr.get(arrayElement).toString());
				bw.write("\n");
				}
				bw.close();

				System.out.println("Done");
	            }catch (IOException e) {
						e.printStackTrace();
					}

	            //System.out.println(jsonMainArr.get(index));
	        	} catch (JSONException | FileNotFoundException je) {
	        		System.out.println(je.toString());
	        }
	    }
	}


