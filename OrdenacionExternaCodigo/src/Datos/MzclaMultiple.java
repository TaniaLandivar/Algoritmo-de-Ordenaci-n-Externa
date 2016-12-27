package Datos;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;

/*
 *
 * @author Tania
 */
public class MzclaMultiple {
    static final int N = 6; 
    static final int N2 = N/2; 
    static File []f = new File[N]; 
    static File f0; 
    static String TOPE = null; 
    static String[] nomf = {"ar1", "ar2", "ar3", "ar4", "ar5", "ar6"}; 
    //método de ordenación
    public void mezcla(String archivo_leer, int campo){
        for (int i = 0; i < N; i++)   
            f[i] = new File(nomf[i]); 
        try {
            int t = distribuir(archivo_leer, campo);
        } catch (IOException ex) {
            Logger.getLogger(MzclaMultiple.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public void mezclaEqMple(String archivo_leer, int campo){
        for (int i = 0; i < N; i++)   
            f[i] = new File(nomf[i]);  
        int i, j, k, k1, t;
        String anterior;
        int []c = new int[N];
        int []cd = new int[N];
        String []r = new String[N2];
        
        Object [] flujos = new Object[N];
        CsvReader flujoEntradaActual = null;
        CsvWriter flujoSalidaActual = null;
        boolean [] actvs = new boolean[N2];
        //distribbución inicial en tramos desde archivo origen
        try{
            t = distribuir(archivo_leer,campo);
            for(i = 0; i < N; i++)
                c[i] = i;
            //bucle hasta número ded tramos == 1: archivo ordenado
            int cont = 0;
            do{
                k1 = (t < N2) ? t : N2;
                for(i = 0; i < k1; i++){
                    flujos[c[i]] = new CsvReader(nomf[c[i]]);
                    cd[i] = c[i];
                }
                j = N2; //indice de archivo de salida
                t = 0;
                for(i = j; i < N;i++)
                    flujos[c[i]] = new CsvWriter(new FileWriter(nomf[c[i]], true), ',');
                    // entrada de una clave de cada flujo
                for( int n = 0; n < k1; n++){
                    flujoEntradaActual = (CsvReader)flujos[cd[n]];
                    flujoEntradaActual.readRecord();
                    r[n] = flujoEntradaActual.get(campo);
                }
                while(k1 > 0){
                    t++; // mezcla de otro tramo
                    for(i = 0; i < k1; i++){
                        actvs[i] = true;
                    }
                    flujoSalidaActual = (CsvWriter)flujos[c[j]];
                    while (!finDeTramos(actvs,k1)){
                        int n;
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
                            r[n] = flujoEntradaActual.get(campo);
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
                        }
                    }
                    j = (j < N-1) ? j+1 : N2; // Siguiente flujo e salida
                }
                for(i = N2; i < N; i++){
                    flujoSalidaActual = (CsvWriter)flujos[c[i]];
                    flujoSalidaActual.close();
                }
                /*     
                    Cambio de finalidad de los flujos: entrada<->salida    
                */ 
                for (i = 0; i < N2; i++){
                    int a; 
                    a = c[i];
                    c[i] = c[i+N2]; 
                    c[i+N2] = a; 
                    File archBor = new File(nomf[c[i+N2]]);
                    archBor.delete();
                }
            }while (t > 1);
            //System.out.print("Archivo ordenado ...  ");
            //System.out.println("Nombre del archivo: "+nomf[c[0]]); 
            File f1 = new File(nomf[c[0]]);
            File f2 = new File("ArchivoOrdenado.csv");
            f1.renameTo(f2);
            for(i = 0; i < N; i++){
                File archBor = new File(nomf[i]);
                archBor.delete();
            }
            JOptionPane.showMessageDialog(null,"Archivo Ordenado");
             
        }
        catch (IOException er) {
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
            anterior = "-9999999";
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
            while(CompararCampos(anterior, clave, campo) == true) {
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
        nt++;  // cuenta ultimo tramo 
        flujo.close();
        for (j = 0; j < N2; j++)  
            flujoSalida[j].close(); 
        
        return nt;
    }
    //devuelve true si no hay tramo activo 
    private static boolean finDeTramos(boolean [] activo, int n) {
        boolean s = true;
        for(int k = 0; k < n; k++) {
            if (activo[k])  s = false; 
        }
        return s; 
    }
    //devuelve el índice del menor valor del array de claves   
    private static int minimo(String [] r, boolean [] activo, int n, int campo) {
        int i, indice;  
        String m = null; 
        
        i = indice = 0; 
        if(campo == 0){
            m = "1000000"; 
        }
        else if(campo == 1){
            m ="zzzzzzzzzzzzzzzz";
        }
        else if(campo == 2){
            m = "TRUE";
        }
        else if(campo == 3){
            m = "31/12/2017";
        }
        for ( ; i < n; i++) {
            if (activo[i] && (CompararCampos(r[i], m, campo) == true)) {
                m = r[i];
                indice = i;     
            }
        }
        return indice; 
    }
    //escribe el contenido del archivo 
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

