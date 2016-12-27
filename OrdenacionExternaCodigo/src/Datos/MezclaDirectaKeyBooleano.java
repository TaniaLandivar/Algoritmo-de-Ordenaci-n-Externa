/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Datos;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 *
 * @author Mateo
 */
public class MezclaDirectaKeyBooleano {
    public MezclaDirectaKeyBooleano(String ruta) throws IOException {
        File fichero = new File(ruta);
        mezclaDirecta(fichero);
    }
    
    private  void mezclaDirecta(File f) throws IOException {
        int longSec;
        int numReg = 0;
        CsvReader bf = new CsvReader(new FileReader(f), ',');
        boolean is_end = bf.readRecord();
        while(is_end){
            numReg++;
            is_end = bf.readRecord();
        }
        System.out.println("Número de registros: "+numReg);
        File f1 = new File("ArchivoAux1.csv");
        File f2= new File("ArchivoAux2.csv");
        longSec = 1;
        while (longSec < numReg)
        {
            distribuir(f, f1, f2, longSec, numReg);
            mezclar(f1, f2, f, longSec, numReg);
            longSec *= 2;
        }
    }
    
    private void distribuir(File f, File f1, File f2,int longSec, int numReg) throws IOException{
        int numSec, resto, i;
        
        CsvReader bf = new CsvReader(new FileReader(f), ',');
        CsvWriter pw1 = new CsvWriter(new FileWriter(f1),',');
        CsvWriter pw2 = new CsvWriter(new FileWriter(f2), ',');
        
        numSec = numReg /(2*longSec);
        resto = numReg %(2*longSec);
       
        for (i = 1; i <= numSec; i++)
        {
            subSecuencia(bf, pw1, longSec);
            subSecuencia(bf, pw2, longSec);
        }
        /*
        Se procesa el resto de registros del archivo
        */
        if (resto > longSec)
            resto -= longSec;
        else
        {
            longSec = resto;
            resto = 0;
        }
        subSecuencia(bf, pw1, longSec);
        subSecuencia(bf, pw2, resto);
        bf.close();
        pw1.close();
        pw2.close();
        
    }
    
    private void subSecuencia(CsvReader f, CsvWriter t,int longSec) throws IOException
    {   
        for (int j = 1; j <= longSec; j++)
        {
            boolean is_end = f.readRecord();
            t.write(f.get(0));
            t.write(f.get(1));
            t.write(f.get(2));
            t.write(f.get(3));
            t.endRecord();
        }
    }
     
     
   private void mezclar(File f1, File f2, File f,int lonSec, int numReg) throws IOException{
        int numSec, resto, i, j, k;
        String clave1 ="",clave2 = "";
        String getClave1="",getClave2="",getClave3="";
        numSec = numReg /(2*lonSec); // número de subsecuencias
        resto = numReg %(2*lonSec);
        
        CsvReader bf1 = new CsvReader(new FileReader(f1), ',');
        CsvReader bf2 = new CsvReader(new FileReader(f2), ',');
        CsvWriter pw = new CsvWriter(new FileWriter(f), ',');
        
        //Claves iniciales  
        boolean is_end =bf1.readRecord();
        clave1=bf1.get(2);
        boolean is_end2 = bf2.readRecord();
        clave2=bf2.get(2);
        
        //Bucle para controlar todo el proceso de mezcla
        for (int s = 1; s <= numSec+1; s++)
        {
            int n1, n2;
            n1 = n2 = lonSec;
            if (s == numSec+1)
            { // proceso de subsecuencia incompleta
                if (resto > lonSec)
                    n2 = resto - lonSec;
                else
                {
                    n1 = resto;
                    n2 = 0;
                }
            }
            i = j = 1;
            while (i <= n1 && j <= n2)
            {
                String clave;
                if (clave1.compareTo(clave2) <0)
                {
                    clave = clave1;
                    getClave1=bf1.get(0);
                    getClave2=bf1.get(1);
                    getClave3=bf1.get(3);

                    try {
                        boolean auxend1=bf1.readRecord();
                        String clave1aux= bf1.get(2);
                        if(auxend1!=false)
                            clave1= clave1aux;
                    }
                    catch(EOFException e){;}
                    i++;
                }
                else
                {
                    clave = clave2;
                    getClave1=bf2.get(0);
                    getClave2=bf2.get(1);
                    getClave3=bf2.get(3);
                    try {
                        boolean auxend2=bf2.readRecord();
                        String clave2aux= bf2.get(2);
                        if(auxend2 !=false)
                            clave2= clave2aux;
                    }
                    catch(EOFException e){;}
                    j++;
                }
                pw.write(getClave1);
                pw.write(getClave2);
                pw.write(clave);
                pw.write(getClave3);
                pw.endRecord();
            }
            /*
            Los registros no procesados se escriben directamente
            */
            for (k = i; k <= n1; k++)
            {   
                getClave1=bf1.get(0);
                getClave2=bf1.get(1);
                getClave3=bf1.get(3);
                pw.write(getClave1);
                pw.write(getClave2);
                pw.write(clave1);
                pw.write(getClave3);
                pw.endRecord();
                try {
                    boolean auxend1=bf1.readRecord();
                    String clave1aux= bf1.get(2);
                    if(auxend1!=false)
                        clave1= clave1aux;
                }
                catch(EOFException e){;}
            }
            for (k = j; k <= n2; k++)
            {
                getClave1=bf2.get(0);
                getClave2=bf2.get(1);
                getClave3=bf2.get(3);
                pw.write(getClave1);
                pw.write(getClave2);
                pw.write(clave2);
                pw.write(getClave3);
                pw.endRecord();
                try {
                    boolean auxend2=bf2.readRecord();
                    String clave2aux= bf2.get(2);
                    if(auxend2!=false)
                       clave2= clave2aux;
                }
                catch(EOFException e){;}
            }
        }
        bf1.close();
        bf2.close();
        pw.close();
    }
}
