package udfs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


/**  
* MixtureTuple.java - create a mixture of one tuple
* Input tuple: {(1,2,3,5)} 
* Output DataBag: {(1,2),(1,3),(1,5),(2,3),(2,5),(3,5)}
* @author Roberto Maestre
* @version 1.0 
*/ 
public class SumDataBag extends EvalFunc<DataBag> {
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

  
     
    public DataBag exec(Tuple input) throws IOException
    {
        try {
            if (!input.isNull()){
                // Create the output like a databag {(res1,res2),(res3,res4)..}
                DataBag output_databag = mBagFactory.newDefaultBag();
                // Unpack tuple in order to get the bag {(1,2),(3,4),...}
                DataBag input_databag = (DataBag) input.get(0);
                try {
                    Iterator<Tuple> iterator = input_databag.iterator();
                    // iterate over elements to flatten the Tuple into a vector
                    double sum = 0;
                    while (iterator.hasNext()){
                        Tuple items = (Tuple) iterator.next();
                        sum += Double.parseDouble(items.get(0).toString());
                    }
                    // Add items to output
                    Tuple items = TupleFactory.getInstance().newTuple(1);
                    items.set(0, Double.toString(sum));
                    output_databag.add(items);
                } catch (Exception e){
                    System.err.println("Error with == 1 ..");
                    return output_databag;
                }
                return output_databag;
            } else {
                return null;
            }
        } catch (Exception e){
            System.err.println("Error with ?? ..");
            DataBag output_databag = mBagFactory.newDefaultBag();
            return output_databag;
        }
    }
}