#Code is used to read the csv file and filter the duplicates

package com.maxis.poc;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Duplicaterecord2 {
    
    public static void main(String a[]) throws IOException{
        
         // main file that contain entrie value
         BufferedReader reader = new BufferedReader(new FileReader("C://Users//saurabhs//PycharmProjects//Test//GeoData//filtered.csv"));
            Set<String> uniqueSet = new HashSet<String>(); // maybe should be bigger
            List<String> duplicateList = new ArrayList<>();
            
            // it store value of file line by line
            String line;
            
            //it compare to uniqueSet 
            int linescounter = 0;
            
            while ((line = reader.readLine()) != null) {

                uniqueSet.add(line);
                linescounter +=1;
                if(linescounter > uniqueSet.size()){
                    duplicateList.add(line);
                    linescounter -=1;
                }
                
            }
            reader.close();
            
             
            //where we store duplicate line in main file
            BufferedWriter duplicatefileWriter = new BufferedWriter(new    FileWriter("C://Users//saurabhs//PycharmProjects//Test//GeoData//filtered_dup.csv"));
            
            for (String duplicateline : duplicateList) {
                duplicatefileWriter.write(duplicateline);
                duplicatefileWriter.newLine();
            }
            duplicatefileWriter.close();
            
            
            BufferedWriter uniuqe = new BufferedWriter(new FileWriter("C://Users//saurabhs//PycharmProjects//Test//GeoData//filtered_uq.csv"));
            
            for (String uniuqeline : uniqueSet) {
                uniuqe.write(uniuqeline);
                uniuqe.newLine();
            }
            uniuqe.close();

    }

    
}
