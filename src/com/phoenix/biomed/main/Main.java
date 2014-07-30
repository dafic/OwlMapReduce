package com.phoenix.biomed.main;

import com.phoenix.biomed.mapreduce.EfficientOwlReasoning;
import com.phoenix.biomed.mapreduce.NaiveOwlReasoning;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author phoenix
 */
public class Main {

    public static void main(String[] args) {
        String PCS = "treatment, possibleDrug, hasTarget, hasAccession, classifiedWith, symbol";
        
        // For Naive Owl Reasoning
        // Requirement: Data should be placed inside '/phoenix/biomed/input1'
        runNaiveOwlReasoning(PCS, args);
        
        // For Efficient Owl Reasoning
        // Requirement: Data should be placed inside '/phoenix/biomed/iinput1'
        //runEfficientOwlReasoning(PCS, args);
    }

    /**
     * Run Naive Owl Reasoning Algorithm
     * @param PCS Initial PCS
     * @param params Args to ToolRunner for MapReduce
     */
    public static void runNaiveOwlReasoning(String PCS, String[] params) {
        int maxLevel = 6;
        try {
            for (int level = 1; level < maxLevel && !isComplete(PCS); level++) {
                int response = ToolRunner.run(new NaiveOwlReasoning(PCS, level), params);

                //update PCS
                PCS = updatePCSI(PCS);
            }

        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /**
     * Run Efficient Owl Reasoning Algorithm
     * @param PCS Initial PCS
     * @param params Args to ToolRunner for MapReduce
     */
    public static void runEfficientOwlReasoning(String PCS, String[] params) {
        
        int maxLevel = 6;
        try {
            for (int level = 1; level < maxLevel && !isComplete(PCS); level++) {
                int pre1 = ToolRunner.run(new EfficientOwlReasoning(PCS, level), params);
                
                //update PCS
                PCS = updatePCSII(PCS);
            }

        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Update PCS for Naive Owl Reasoning Algorithm
     *
     * @param PCS Initial PCS string
     * @return Updated PCS string
     */
    public static String updatePCSI(String PCS) {
        String[] split = PCS.split(",");
        String newPCS = split[0].trim();
        if (split.length > 1) {
            newPCS += "|" + split[1].replaceAll("[ ]+", "");
            // append the rest opc
            for (int i = 2; i < split.length; i++) {
                newPCS += ", " + split[i].trim();
            }
        }
        return newPCS;
    }

    /**
     * Update PCS for Efficient Owl Reasoning Algorithm
     *
     * @param PCS Initial PCS string
     * @return Updated PCS string
     */
    public static String updatePCSII(String PCS) {
        String[] split = PCS.split(",");
        String newPCS = "";
        if (split.length % 2 == 0) {
            for (int i = 0; i < split.length; i = i + 2) {
                newPCS += ", " + split[i] + "|" + split[i + 1].trim();
            }
        } else {
            for (int i = 0; i < split.length - 1; i = i + 2) {
                newPCS += ", " + split[i] + "|" + split[i + 1].trim();
            }
            newPCS += ", " + split[split.length - 1].trim();
        }
        return newPCS.substring(2);
    }

    /**
     * Returns true if there is only one OPC in PCS. Indicates end of iteration.
     *
     * @param PCS
     * @return true if there is only one OPC in PCS
     */
    public static boolean isComplete(String PCS) {
        return !PCS.contains(",");
    }
}
