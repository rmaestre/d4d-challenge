/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package datastructures;


/**
 *
 * @author rmaestre
 */
public class Point {
    
    public final double x; 
    public final double y; 
    
    public Point(double x, double y) { 
        this.x = x; 
        this.y = y; 
    } 
    
    @Override public String toString() {
        return "["+Double.toString(x)+","+Double.toString(y)+"]";
    }
    
    public double get_euclidean_distance(Point p) {
            return Math.sqrt(Math.pow(this.x - p.x, 2) + Math.pow(this.y - p.y, 2));
    }
}
