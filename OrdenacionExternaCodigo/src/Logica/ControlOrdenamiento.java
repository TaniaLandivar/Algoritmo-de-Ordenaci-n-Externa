/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Logica;

import Datos.MzclaMultiple;
import Datos.MezclaDirecta;
import Datos.OrdNaturalCSV;
import Datos.Polifasica;
import java.io.IOException;
import javax.swing.JOptionPane;

/**
 *
 * @author Tania
 */
public class ControlOrdenamiento {
    public static void controlOrdenamiento(String ruta_archivo, int metodo, int campo) throws Exception{
        if(ruta_archivo == null){
            JOptionPane.showMessageDialog(null,"Seleccione un archivo");   
        }
        else if(metodo == 0){
            JOptionPane.showMessageDialog(null,"Seleccione un metodo");   
        }
        else if(campo == -1){
            JOptionPane.showMessageDialog(null,"Seleccione el campo");   
        }
        else{
            if(metodo == 1){
                new MezclaDirecta().MezclaDirecta(ruta_archivo, campo);
            }
            else if(metodo == 2){
                OrdNaturalCSV ordNat = new OrdNaturalCSV(ruta_archivo,campo);
                JOptionPane.showMessageDialog(null,"Archivo Ordenado");
            }
            else if(metodo == 3){
               //new MzclaMultiple().mezcla(ruta_archivo, campo);
               new MzclaMultiple().mezclaEqMple(ruta_archivo, campo);
                
            }
            else if(metodo == 4){
               //new Polifasica().mezcla(ruta_archivo, campo);
               new Polifasica().mezclaPolifasica(ruta_archivo, campo);
            }
            else if(metodo == 5){
                
            }
        } 
    }
    
}
