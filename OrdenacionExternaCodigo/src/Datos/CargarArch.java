/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Datos;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;

/**
 *
 * @author Christian
 */
public class CargarArch {
 
    JFileChooser fileChooser = new JFileChooser();
    
    public CargarArch(){
    }
    public String cargar(){
        fileChooser.showOpenDialog(fileChooser);
        File archivo = fileChooser.getSelectedFile();
        return archivo.getPath();
    }
}
