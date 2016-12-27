/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Datos;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

/**
 *
 * @author Christian
 */
public class OrdNaturalCSV {
    public int campo;
    public String nombre_archivo;
    /**
     * @param args the command line arguments
     */
    
    public OrdNaturalCSV(String archivo , int camp) throws Exception{
        this.campo = camp;
        this.nombre_archivo = archivo;
        this.ordenar(nombre_archivo);
    }
    //Cuenta los registros de los cada Archivo para poder recorrer(se usa en los while)
    public int contarRegistros(int seleccion) {
        int index = 0;
        if (seleccion == 1) {

            index = 0;
            CsvReader archivo = null;
            try {
                archivo = new CsvReader("Auxiliar1.csv");

                while (archivo.readRecord()) {
                    index += 1;
                }
                archivo.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            
        }

        if (seleccion == 2) {
            index = 0;
            CsvReader archivo = null;
            try {
                archivo = new CsvReader("Auxiliar2.csv");

                while (archivo.readRecord()) {
                    index += 1;
                }
                archivo.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            
        }
        return index;
    }
    
    public void ordenar(String nombreArchivo) throws Exception {
        while (separacion(nombreArchivo, "Auxiliar1.csv", "Auxiliar2.csv")) {
            mezcla(nombreArchivo, "Auxiliar1.csv", "Auxiliar2.csv");
        }
    }

    //Metodo para generar particiones de secuencias
    public boolean separacion(String nombreArchivo, String archivo1, String archivo2) {
        //Variables para guardar y manejar los campos
        String actual = null;
        String anterior = null;
        //Variable para controlar el indice del archivo al cual se va a escribir.
        int indexArchivo = 0;
        //Variable que determina si existe un cambio de secuencia en el ordenamiento
        boolean particion = false;
        //Objetos CsvWriter para los archivos auxiliares y CsvReader para el principal
        CsvWriter Auxiliares[] = new CsvWriter[2];
        CsvReader Archivo = null;
        try {
            //Abrir los archivos a manejar
            Auxiliares[0] = new CsvWriter(new FileWriter(archivo1, false), ' ');//TRUE
            Auxiliares[1] = new CsvWriter(new FileWriter(archivo2, false), ' ');//TRUE
            Archivo = new CsvReader(nombreArchivo);

            //lee cada registro del archivo
            while (Archivo.readRecord()) {
                anterior = actual;
                actual = Archivo.get(campo);
                String registro = Archivo.getRawRecord();
                if (anterior == null) {
                    anterior = actual;
                }
                //Compara cada campo siempre dependiendo del tipo
                if ( CompararCampos(anterior,actual)==false){
                    //selecciona el archivo auxiliar a escribir
                    indexArchivo = indexArchivo == 0 ? 1 : 0;
                    particion = true;
                }
                //imprime en el archivo auxiliar dependiendo del indice
                Auxiliares[indexArchivo].write(registro);
                Auxiliares[indexArchivo].endRecord();
            }
            Archivo.close();
            Auxiliares[0].close();
            Auxiliares[1].close();
        } catch (FileNotFoundException e) {
            System.out.println("Error lectura/escritura");
        } catch (IOException e) {
            System.out.println("Error en la creacion o apertura del archivo 1");
        }
        //retorna un booleano(true) para seguir con las particiones y fusiones
        return particion;
    }

    //Metodo de mezcla de los datos obtenidos en el metodo de separacion
    public void mezcla(String nombreArchivo, String archivo1, String archivo2) {
        //Variables para almacenar los datos de los archivos dependiendo de los archivos auxiliares
        String[] actual = new String[2];
        String[] anterior = new String[2];
        String[] registro = new String[2];
        boolean[] finArchivo = new boolean[2];
        int indexArchivo = 0;
        //Objetos CsvWriter para el archivo principal y CsvReader para los auxiliares
        CsvReader Auxiliares[] = new CsvReader[2];
        CsvWriter Archivo = null;

        boolean alreadyExists = new File(nombreArchivo).exists();
        if (alreadyExists) {
            File ArchivoEmpleados = new File(nombreArchivo);
            ArchivoEmpleados.delete();
        }

        try {
            //cuenta los registros cada vez de los archivos para poder usarlos en el while e ir recorriendo los archivos
            int contAux1 = contarRegistros(1);
            int contAux2 = contarRegistros(2);
            boolean primeraFusion= false;
            //Abrir los acrhivos 
            Auxiliares[0] = new CsvReader(archivo1);
            Auxiliares[1] = new CsvReader(archivo2);
            Archivo = new CsvWriter(new FileWriter(nombreArchivo, false), ' ');
            
            while (contAux1 > 0 && contAux2 > 0) {
                // 1era vez: inicializar con la primera palabra de cada archivo
                if (primeraFusion==false) {
                    Auxiliares[0].readRecord();
                    actual[0] = Auxiliares[0].get(campo);
                    registro[0] = Auxiliares[0].getRawRecord();
                    contAux1--;
                    Auxiliares[1].readRecord();
                    actual[1] = Auxiliares[1].get(campo);
                    registro[1] = Auxiliares[1].getRawRecord();
                    contAux2--;
                    primeraFusion=true;
                }
                // al inicio del procesamiento de dos secuencias, anterior y actual apuntan a la primer palabra de cada secuencia.
                anterior[0] = actual[0];
                anterior[1] = actual[1];

                //Mezclar las dos secuencias hasta que se acaben
                while (CompararCampos(anterior[0], actual[0])   &&  CompararCampos(anterior[1], actual[1])) {
                    indexArchivo = (CompararCampos(actual[0], actual[1])) ? 0 : 1; 
                    Archivo.write(registro[indexArchivo]);
                    anterior[indexArchivo] = actual[indexArchivo];
                    Archivo.endRecord();
                    //Salir del while cuando no haya datos, pero ya procesamos el ultimo nombre del archivo
                    if(indexArchivo==0){
                        if (contAux1>0) {
                            Auxiliares[0].readRecord();
                            actual[0] = Auxiliares[0].get(campo);
                            registro[0]=Auxiliares[0].getRawRecord();
                            contAux1--;
                        } else {
                            finArchivo[0] = true;
                            break;
                        }
                    }
                    if(indexArchivo==1){
                        if (contAux2>0) {
                            Auxiliares[1].readRecord();
                            actual[1] = Auxiliares[1].get(campo);
                            registro[1]=Auxiliares[1].getRawRecord();
                            contAux2--;
                        } else {
                            finArchivo[1] = true;
                            break;
                        }
                    }
                    

                }

                /* indexArchivo nos indica que archivo causo  que salieramos del while anterior
                , por lo que tenemos que purgar el otro archivo */
                if(indexArchivo == 0 ){
                    while (CompararCampos(anterior[1], actual[1])) {
                        Archivo.write(registro[1]);
                        anterior[1] = actual[1];
                        Archivo.endRecord();
                        if (contAux2>0) {
                            Auxiliares[1].readRecord();
                            actual[1] = Auxiliares[1].get(campo);
                            registro[1]=Auxiliares[1].getRawRecord();
                            contAux2--;
                        } else {
                            finArchivo[1] = true;
                            break;
                        }
                    }
                }
                if(indexArchivo == 1){
                    while (CompararCampos(anterior[0], actual[0])) {  
                        Archivo.write(registro[0]);
                        anterior[0] = actual[0];
                        Archivo.endRecord();
                        if (contAux1>0) {
                            Auxiliares[0].readRecord();
                            actual[0] = Auxiliares[0].get(campo);
                            registro[0]=Auxiliares[0].getRawRecord();
                            contAux1--;
                        } else {
                            finArchivo[0] = true;
                            break;
                        }
                    }
                }
                
                
            }

            // Purgar los dos archivos en caso de que alguna secuencia haya quedado sola al final del archivo.
            if (!finArchivo[0]) {
                Archivo.write(registro[0]);
                Archivo.endRecord();
                while (contAux1>0) {
                    Auxiliares[0].readRecord();
                    Archivo.write(Auxiliares[0].getRawRecord());
                    Archivo.endRecord();
                    contAux1--;
                }
            }

            if (!finArchivo[1]) {
                Archivo.write(registro[1]);
                Archivo.endRecord();
                while (contAux2>0) {
                    Auxiliares[1].readRecord();
                    Archivo.write(Auxiliares[1].getRawRecord());
                    Archivo.endRecord();
                    contAux2--;
                }
            }
            Auxiliares[0].close();
            Auxiliares[1].close();
            Archivo.close();
        } catch (FileNotFoundException e) {
            System.err.println(e);
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    boolean CompararCampos(String camp1, String camp2){
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
