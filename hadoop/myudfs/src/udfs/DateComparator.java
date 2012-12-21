/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package udfs;

import datastructures.Tupla;
import java.util.Comparator;

/**
 *
 * @author rmaestre
 */
public class DateComparator implements Comparator<Tupla> {
    @Override
    public int compare(Tupla t1, Tupla t2) {
        return t1.date.compareTo(t2.date);
    }
}
