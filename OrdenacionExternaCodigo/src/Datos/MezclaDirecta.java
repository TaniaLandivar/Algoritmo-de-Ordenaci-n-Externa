/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Datos;

import java.io.IOException;
import java.text.ParseException;
import java.util.Scanner;
import javax.swing.JOptionPane;

/**
 *
 * @author Mateo
 */
public class MezclaDirecta {

    /**
     * @param args the command line arguments
     */
    public void MezclaDirecta(String ruta, int campo) throws IOException, ParseException {
        Scanner sc = new Scanner(System.in);
        MezclaDirectaKeyNombres nombres;
        MezclaDirectaKeyNumero numero;
        MezclaDirectaKeyBooleano booleano;
        MezclaDirectaKeyFecha fecha;
        switch (campo) {
            case 0 :
                numero=new MezclaDirectaKeyNumero(ruta);
                JOptionPane.showMessageDialog(null,"Archivo Ordenado");
                break;
                
            case 1:
                nombres=new MezclaDirectaKeyNombres(ruta);
                JOptionPane.showMessageDialog(null,"Archivo Ordenado");
                break;
                
            case 2:
                booleano=new MezclaDirectaKeyBooleano(ruta);
                JOptionPane.showMessageDialog(null,"Archivo Ordenado");
                break;
            case 3:
                fecha=new MezclaDirectaKeyFecha(ruta);
                JOptionPane.showMessageDialog(null,"Archivo Ordenado");
                break;
            default:
                System.out.println("Opción Incorrecta!");
                break;
        }
        
    }
}
