package com.phoenix.biomed.common;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author phoenix
 */
public class PCS {
    String pcs;
    List<String> pcsList = new ArrayList<String>();
    
    /**
     * 
     * @param pcs comma separated string of pcs
     * e.g treatment, possibleDrug, hasTarget
     */
    public PCS(String pcs){
        this.pcs = pcs;
        for(String opc: pcs.split(",")){
            pcsList.add(opc.replaceAll("[ ]+", ""));
        }
    }
    
    public String get0(){
        return pcsList.get(0);
    }
    
    public String get1(){
        return pcsList.get(1);
    }
    
    public String get(int i){
        return pcsList.get(i);
    }
    
    public int size(){
        return pcsList.size();
    }
    
    public boolean belongs(String opc){
        return pcsList.contains(opc);
    }
    
    public int getPID(String opc){
        if(pcsList.contains(opc)){
            return pcsList.indexOf(opc);
        }else{
            return -1;
        }
    }
    
    public static void main(String[] args) {
        String pcsString = "treatment, possibleDrug, hasTarget, hasAccession, classifiedWith, symbol";
        PCS pcs = new PCS(pcsString);
        
        System.out.println("size:"+pcs.size());
        System.out.println("p0:"+pcs.get0());
        System.out.println("p1:"+pcs.get1());
        System.out.println("belongs 'treatment':"+pcs.belongs("treatment"));
        System.out.println("belongs 'geneSequence':"+pcs.belongs("geneSequence"));
        System.out.println("pid[treatment]:"+pcs.getPID("treatment"));
        System.out.println("pid[treatment]:"+pcs.getPID("symbol"));
        System.out.println("pid[treatment]:"+pcs.getPID("geneSequence"));
    }
    
}
