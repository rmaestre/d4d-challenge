package udfs;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


/**  
* WeekDayDetection.java - get a string formated date, and return the number of
*                           the week. (0=Monday, ..., 6=Sunday)
* Input tuple: {(1,2,3,5)} 
* Output DataBag: {(1,2),(1,3),(1,5),(2,3),(2,5),(3,5)}
* @author Roberto Maestre
* @version 1.0 
*/ 
public class WeekDayDetection extends EvalFunc<DataBag> {
    
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

     
    public DataBag exec(Tuple input) throws IOException
    {
        try {
            if (!input.isNull()){
                // Create the output like a databag {(res1,res2),(res3,res4)..}
                DataBag output_databag = mBagFactory.newDefaultBag();
                // Unpack tuple in order to get the bag {(1,2),(3,4),...}
                String input_time = (String) input.get(0);
                try {

                    DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy kk:mm:ss");
                    Date date = formatter.parse(String.format("%s/%s/%s %s:%s:%s", 
                                            input_time.substring(5, 7),
                                            input_time.substring(8, 10),
                                            input_time.substring(0, 4),
                                            input_time.substring(11, 13),
                                            input_time.substring(14, 16),
                                            input_time.substring(17, 18)));
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime(date);
                    int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
                    int dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
                    int hour = calendar.get(Calendar.HOUR_OF_DAY);
                    
                    // Add items to output
                    Tuple items = TupleFactory.getInstance().newTuple(1);
                    items.set(0, String.format("%d:%d:%d", dayOfWeek, dayOfMonth, hour));
                    output_databag.add(items);
                    
                } catch (Exception e){
                    Tuple items = TupleFactory.getInstance().newTuple(1);
                    items.set(0, "petting #1"+e.getMessage());
                    output_databag.add(items);
                    return output_databag;
                }
                return output_databag;
            } else {
                DataBag output_databag = mBagFactory.newDefaultBag();
                Tuple items = TupleFactory.getInstance().newTuple(1);
                items.set(0, "petting #2");
                output_databag.add(items);
                return output_databag;
            }
        } catch (Exception e){
            System.err.println("Error with ?? ..");
            DataBag output_databag = mBagFactory.newDefaultBag();
            Tuple items = TupleFactory.getInstance().newTuple(1);
            items.set(0, "petting #3"+e.getMessage());
            output_databag.add(items);
            return output_databag;
        }
    }
    
}
