/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datastructures;

import java.util.Calendar;

/**
 *
 * @author rmaestre
 */
public class Tupla {
    public final Calendar date; 
    public final String subprefecture; 
    
    public Tupla(Calendar x, String y) { 
        this.date = x; 
        this.subprefecture = y; 
    } 
    
    @Override public String toString() {
        //return "["+this.date.getTime().toString()+" , "+this.subprefecture+"]";
        return this.subprefecture;
    }
}
