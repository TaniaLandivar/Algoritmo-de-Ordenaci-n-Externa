package Datos;

import static Datos.MzclaMultiple.N;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;
public class Polifasica
{     
     
    static final int N = 3;
    static final int N2 = N - 1;
    static String nomf [] = new String [N]; 
    static File f0;
    static File []f = new File[N];
    public void mezclaPolifasica(String archivo_leer, int campo)
    {
        //Crea nombres de archivo
        for (int i = 0; i < N; i++)
            nomf[i] = String.valueOf(i);
        //Cran los archivos con los nombres
        for (int i = 0; i < N; i++)   
            f[i] = new File(nomf[i]);
        int i, j, k, k1, t, n = -1;
        String anterior;
        int [] c = new int[N];
        int [] cd = new int[N];
        String [] r = new String[N2];
        Object [] flujos = new Object[N];
        CsvReader flujoEntradaActual = null;
        CsvWriter flujoSalidaActual = null;
        boolean [] actvs = new boolean[N2];
        try {
            /*
               Numero de Interacciones que va a tener para ordenar los datos
            */
               for (i = 0; i < N; i++)
                        c[i] = i;
               t = distribuir(archivo_leer,campo);
                // bucle hasta número de tramos == 1: archivo ordenado
                do {
                        k1 = (t < N2) ? t : N2;
                        for (i = 0; i < k1; i++)
                        {
                            flujos[c[i]] = new CsvReader(nomf[c[i]]);
                            cd[i] = c[i];
                        }
                        j = N-1; // índice de archivo de salida
                 //      System.out.println(" Salida : " + j);

                        t = 0;
                        flujos[j] = new CsvWriter(new FileWriter(nomf[c[j]], true), ',');
                        // entrada de una clave de cada flujo
                        for (int p = 0; p < k1; p++)
                        { 
                            if(p != n){
                                flujoEntradaActual = (CsvReader)flujos[cd[p]];
                                flujoEntradaActual.readRecord();
                                r[p] = flujoEntradaActual.get(campo);
                                
                            }
                       
                        }

                        while (k1 > 0)
                        {
                            t++; // mezcla de otro tramo
                            for (i = 0; i < k1; i++)
                                    actvs[i] = true;
                            flujoSalidaActual = (CsvWriter)flujos[j];
                            while (!finDeTramos(actvs, k1)) 
                            {
                                n = minimo(r, actvs, k1,campo);
                                flujoEntradaActual = (CsvReader)flujos[cd[n]];
                                flujoSalidaActual.write(flujoEntradaActual.get(0));
                                flujoSalidaActual.write(flujoEntradaActual.get(1));
                                flujoSalidaActual.write(flujoEntradaActual.get(2));
                                flujoSalidaActual.write(flujoEntradaActual.get(3));
                                flujoSalidaActual.endRecord();
                                anterior = r[n];
                                flujoEntradaActual.readRecord();
                                if(flujoEntradaActual.get(2) != "") {
                                    System.out.println("anterior: " + anterior);
                                    r[n] = flujoEntradaActual.get(campo);
                                    System.out.println("r"+r[n]);
                                    if (CompararCampos(anterior, r[n], campo) == false) // fin de tramo
                                        actvs[n] = false;
                                }
                                else{
                                    k1--; 
                                    flujoEntradaActual.close(); 
                                    cd[n] = cd[k1]; 
                                    r[n] = r[k1];
                                    actvs[n] = actvs[k1]; 
                                    actvs[k1] = false;// no se accede a posición k1
                                    File archBor = new File(nomf[n]);
                                    archBor.delete();
                                    CsvWriter flujos_Aux= new CsvWriter(new FileWriter(nomf[n], true), ',');
                                    j = n;
                                }
                            } 
                        }
                       flujoSalidaActual = (CsvWriter)flujos[j];
                       flujoSalidaActual.close();
             
                } while (t > 1);
               // System.out.print("Archivo ordenado ...");
                escribir(nomf[c[j]]);
                for(i = 0; i < N; i++){
                File archBor = new File(nomf[i]);
                    archBor.delete();
                }
        }	
        catch (IOException er)
        {
                er.printStackTrace();
        }

    }
         
    //distribuye tramos de flujos de entrada en flujos de salida 
    private static int distribuir(String archivo_leer,int campo) throws IOException{
        int j, nt; 
        String anterior = null;
        String clave; 
        CsvReader flujo = new CsvReader(archivo_leer);
        CsvWriter []flujoSalida= new CsvWriter[N2];
        if(campo == 0){
            anterior = "-99999";
        }
        else if(campo == 1){
            anterior = "@" ;
        }
        else if(campo == 2){
            anterior = "false";
        }
        else if(campo == 3){
            anterior = "11/12/1100";
        }
        for (j = 0; j < N2; j++) {
            flujoSalida[j] = new CsvWriter(new FileWriter(f[j], true), ',');          
        }
        j = 0;   // indice del flujo de salida 
        nt = 0; 
        int contador_pasadas = 0;
        // bucle termina con la excepción fin de fichero 
        while (flujo.readRecord()) {
            contador_pasadas++;
            clave =  flujo.get(campo);
            System.out.println("Anterior y Clave: "+anterior +"/"+ clave);
            while(CompararCampos(anterior, clave, campo) == true) {
                System.out.println("Anterior y Clave1: "+anterior +"/"+ clave);
                contador_pasadas++;
                flujoSalida[j].write(flujo.get(0));
                flujoSalida[j].write(flujo.get(1));
                flujoSalida[j].write(flujo.get(2));
                flujoSalida[j].write(flujo.get(3));
                flujoSalida[j].endRecord();
                anterior = clave;
                flujo.readRecord();
                if(flujo.get(0) == ""){
                    break;
                }
                clave = flujo.get(campo);
                System.out.println("clave: "+clave);
            }
            if(flujo.get(0) != ""){
                nt++; // nuevo tramo 
                j = (j < N2-1) ? j+1 : 0; // siguiente archivo
                flujoSalida[j].write(flujo.get(0));
                flujoSalida[j].write(flujo.get(1));
                flujoSalida[j].write(flujo.get(2));
                flujoSalida[j].write(flujo.get(3));
                flujoSalida[j].endRecord();
                anterior = clave;
            }
        }
        System.out.println("Contador de pasadas: "+contador_pasadas);
        nt++;  // cuenta ultimo tramo 
        System.out.println("\n*** Número de tramos: " + nt + " ***"); 
        flujo.close();
        for (j = 0; j < N2; j++)  
            flujoSalida[j].close(); 
        
        return nt; //Numero de tramos
    }
	  //devuelve el índice del menor valor del array de claves   
    private static int minimo(String [] r, boolean [] activo, int n, int campo) {
        int i, indice;  
        String m = null; 
        
        i = indice = 0; 
        if(campo == 0){
            m = "10000"; 
        }
        else if(campo == 1){
            m ="zzzzzzzzzzzzz";
        }
        else if(campo == 2){
            m = "true";
        }
        else if(campo == 3){
            m = "31/12/2016";
        }
        for ( ; i < n; i++) {
            if (activo[i] && (CompararCampos(r[i], m, campo) == true)) {
                m = r[i];
                indice = i;     
            }
        }
        return indice; 
    }
    //devuelve true si no hay tramo activo
    private static boolean finDeTramos(boolean [] activo, int n)
    {
        boolean s = true;

        for(int k = 0; k < n; k++)
        {
                if (activo[k])	
                   s = false;
        }
        return s;
    }
    //escribe las claves del archivo
    static void escribir(String archivo_leer) {
        int clave, k;
        boolean mas = true; 
        try { 
            CsvReader leer_datos = new CsvReader(archivo_leer);
            while (leer_datos.readRecord()) {
                System.out.println(leer_datos.get(0)+" "+leer_datos.get(1)+" "+leer_datos.get(2)+" "+leer_datos.get(3));
            } 
            leer_datos.close();
            
        }catch (IOException e) {
            System.out.println("Error entrada/salia durante el proceso "+ "de ordenación " );
            e.printStackTrace();
        }  
    } 
    static boolean CompararCampos(String camp1, String camp2, int campo){
       int a=0, b=0;
       if(campo==0){ // para cuando los campos deban ser numericos
           a=Integer.parseInt(camp1);
           b=Integer.parseInt(camp2);
       }
       else if(campo==3){ //para cuando los campos sonde formaot fecha
           try{
               SimpleDateFormat fecha = new SimpleDateFormat("dd/MM/yyyy");
               Date fecha1 = fecha.parse(camp1);
               Date fecha2 = fecha.parse(camp2);
                if ( fecha1.before(fecha2)   ||  (fecha1.equals(fecha2))){
                   a=0;
                   b=1;
                }  
                else{
                   a=1;
                   b=0;
               }

           }
          catch (ParseException e) {
          }

       }
       else{//para cuando los campos sea letras(string mismo)
           if(camp1.compareTo(camp2)<=0){
               a=0;
               b=1;
           }
           else{
               a=1;
               b=0;
           }
       }

       if (a <= b){//compara las condiciones para cada campo
           return true;
       }
           return false;

    } 
}