/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package udfs;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import nlp_utils.Clipping;
import nlp_utils.TupleObject;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.apache.commons.logging.Log.*;

/**
 *
 * @author rmaestre
 * 
*/
public class NLP extends EvalFunc<DataBag> {

    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {
        // TODO Auto-generated method stub
        // Get all parameters from UDF call
        String idras = (String) input.get(0);
        String url = (String) input.get(1);
        String keys = (String) input.get(2);
        String canal = (String) input.get(3);
        String time = (String) input.get(4);
        String texto = (String) input.get(5);

        //log.warn("Longitud texto: "+texto.length());
        
        // Check restrictions, all fieds are required
        if (idras != null && idras.length() > 0 && url != null && url.length() > 0
                && keys != null && keys.length() > 0 && canal != null && canal.length() > 0
                && time != null && time.length() > 0 && texto != null && texto.length() > 0) {

            // Create Databag type output object
            DataBag output = null;
            // Get the whole mentions (clippings)
            Collection<TupleObject> result = Clipping.getMentions(texto, keys);
            // Check for results (List with multiple tuples (<a,b>, <a,c>, ....))
            if (result.size() > 0) {
                Iterator<TupleObject> i = result.iterator();
                // Iterate over each tuple in the list
                output = mBagFactory.newDefaultBag();
                while (i.hasNext()) {
                    TupleObject tuple = (TupleObject) i.next();
                    Tuple items = TupleFactory.getInstance().newTuple(7);
                    // Check for each key if exists in text
                    items.set(0, idras.toString());
                    items.set(1, url.toString());
                    items.set(2, canal.toString());
                    items.set(3, time.toString());
                    // Add key
                    items.set(4, tuple.getFirst().toString());
                    // Add synonymous
                    items.set(5, tuple.getSecond().toString());
                    // Add clean line
                    items.set(6, tuple.getThird().toString());
                    // Add output to the databag
                    output.add(items);
                }
            }
            if (output == null) {
                Tuple items = TupleFactory.getInstance().newTuple(7);
                output = mBagFactory.newDefaultBag();
                output.add(items);
            }
            //og.warn("Menciones detectadas: "+output.size());
            return output;
        } else {
            Tuple items = TupleFactory.getInstance().newTuple(7);
            DataBag output = null;
            output = mBagFactory.newDefaultBag();
            output.add(items);
            return output;
        }
    }
}