package udfs;

import datastructures.Point;
import datastructures.Tupla;
import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


/**  
* UserTraceLenght.java - Get a user trace, sort traces, and calculate distance
* Input tuple: {(week,user_id), {(1,2,3,5),(),....}} 
* Output DataBag: {(1,2),(1,3),(1,5),(2,3),(2,5),(3,5)}
* @author Roberto Maestre
* @version 1.0 
*/ 
public class UserTraceLenght extends EvalFunc<DataBag> {
    
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

    
    public ArrayList<Point> getLatLon(){
        ArrayList<Point> latlon = new ArrayList<Point>();
            latlon.add(0, null);
            latlon.add(1, new Point(-3.260397,6.906417));
            latlon.add(2, new Point(-3.632290,6.907771));
            latlon.add(3, new Point(-3.397551,6.426104));
            latlon.add(4, new Point(-3.662953,6.660800));
            latlon.add(5, new Point(-3.440788,6.937723));
            latlon.add(6, new Point(-3.291995,6.328551));
            latlon.add(7, new Point(-3.366372,7.182663));
            latlon.add(8, new Point(-3.498494,7.166416));
            latlon.add(9, new Point(-3.149608,7.015214));
            latlon.add(10, new Point(-3.787625,6.209023));
            latlon.add(11, new Point(-3.953336,5.947493));
            latlon.add(12, new Point(-4.023325,6.321925));
            latlon.add(13, new Point(-3.839996,6.472760));
            latlon.add(14, new Point(-4.018499,5.876154));
            latlon.add(15, new Point(-3.800610,5.946270));
            latlon.add(16, new Point(-3.570096,6.069357));
            latlon.add(17, new Point(-4.344698,6.077307));
            latlon.add(18, new Point(-4.513142,5.846961));
            latlon.add(19, new Point(-4.105984,6.101675));
            latlon.add(20, new Point(-4.400224,6.194517));
            latlon.add(21, new Point(-4.038918,5.654023));
            latlon.add(22, new Point(-5.723872,7.052564));
            latlon.add(23, new Point(-6.055082,6.602947));
            latlon.add(24, new Point(-5.844579,6.635363));
            latlon.add(25, new Point(-5.636775,6.651568));
            latlon.add(26, new Point(-6.037546,6.896568));
            latlon.add(27, new Point(-5.500270,6.723468));
            latlon.add(28, new Point(-6.111412,7.448124));
            latlon.add(29, new Point(-5.539035,7.657708));
            latlon.add(30, new Point(-5.606104,7.493205));
            latlon.add(31, new Point(-5.476948,7.914011));
            latlon.add(32, new Point(-5.703679,7.806143));
            latlon.add(33, new Point(-4.315782,8.379720));
            latlon.add(34, new Point(-4.137540,8.081339));
            latlon.add(35, new Point(-4.530515,8.018313));
            latlon.add(36, new Point(-4.375863,7.960166));
            latlon.add(37, new Point(-4.750311,8.371633));
            latlon.add(38, new Point(-4.675715,8.731388));
            latlon.add(39, new Point(-4.938431,7.791673));
            latlon.add(40, new Point(-4.701989,7.714015));
            latlon.add(41, new Point(-5.166847,7.838461));
            latlon.add(42, new Point(-5.277931,7.695516));
            latlon.add(43, new Point(-5.300993,7.934304));
            latlon.add(44, new Point(-5.102958,7.457994));
            latlon.add(45, new Point(-4.908034,8.106257));
            latlon.add(46, new Point(-5.232071,8.319269));
            latlon.add(47, new Point(-5.598895,8.725664));
            latlon.add(48, new Point(-5.181111,9.163139));
            latlon.add(49, new Point(-5.531590,7.361837));
            latlon.add(50, new Point(-5.308574,7.450059));
            latlon.add(51, new Point(-5.645342,7.360556));
            latlon.add(52, new Point(-5.190957,7.136901));
            latlon.add(53, new Point(-5.229103,6.527132));
            latlon.add(54, new Point(-5.112736,6.417422));
            latlon.add(55, new Point(-5.026409,6.699474));
            latlon.add(56, new Point(-5.148486,6.919306));
            latlon.add(57, new Point(-4.884319,7.225912));
            latlon.add(58, new Point(-5.337015,6.823865));
            latlon.add(59, new Point(-3.636521,5.654811));
            latlon.add(60, new Point(-4.002729,5.334637));
            latlon.add(61, new Point(-3.844322,5.357087));
            latlon.add(62, new Point(-3.701841,5.401422));
            latlon.add(63, new Point(-4.242893,5.414191));
            latlon.add(64, new Point(-4.066367,5.517012));
            latlon.add(65, new Point(-4.559908,5.238031));
            latlon.add(66, new Point(-4.535666,5.406788));
            latlon.add(67, new Point(-4.541333,5.657780));
            latlon.add(68, new Point(-4.998122,5.365156));
            latlon.add(69, new Point(-4.754623,5.984212));
            latlon.add(70, new Point(-5.072439,6.204175));
            latlon.add(71, new Point(-7.136376,7.570488));
            latlon.add(72, new Point(-7.289982,7.673955));
            latlon.add(73, new Point(-7.183199,7.316389));
            latlon.add(74, new Point(-7.344393,7.405325));
            latlon.add(75, new Point(-7.590199,7.451152));
            latlon.add(76, new Point(-7.465907,7.267746));
            latlon.add(77, new Point(-7.622609,7.142829));
            latlon.add(78, new Point(-7.809244,7.291920));
            latlon.add(79, new Point(-8.177766,6.958076));
            latlon.add(80, new Point(-8.291545,6.722375));
            latlon.add(81, new Point(-7.396248,7.142536));
            latlon.add(82, new Point(-7.475430,7.003247));
            latlon.add(83, new Point(-7.398694,7.251394));
            latlon.add(84, new Point(-8.184913,7.423305));
            latlon.add(85, new Point(-7.982719,7.195188));
            latlon.add(86, new Point(-7.199843,7.074423));
            latlon.add(87, new Point(-7.948533,7.868612));
            latlon.add(88, new Point(-5.723696,9.355952));
            latlon.add(89, new Point(-5.698190,9.562844));
            latlon.add(90, new Point(-5.991164,9.618970));
            latlon.add(91, new Point(-5.836502,8.866821));
            latlon.add(92, new Point(-5.935291,10.036309));
            latlon.add(93, new Point(-6.103258,9.147285));
            latlon.add(94, new Point(-5.395215,9.534156));
            latlon.add(95, new Point(-5.428445,9.297756));
            latlon.add(96, new Point(-5.573247,9.075830));
            latlon.add(97, new Point(-6.409845,10.464733));
            latlon.add(98, new Point(-6.605562,10.494488));
            latlon.add(99, new Point(-6.267314,10.360861));
            latlon.add(100, new Point(-6.648476,9.828754));
            latlon.add(101, new Point(-6.499432,9.395949));
            latlon.add(102, new Point(-6.219072,9.834516));
            latlon.add(103, new Point(-6.373818,10.043771));
            latlon.add(104, new Point(-6.543058,10.023793));
            latlon.add(105, new Point(-4.942000,9.548070));
            latlon.add(106, new Point(-5.183377,9.715982));
            latlon.add(107, new Point(-4.400180,9.150170));
            latlon.add(108, new Point(-5.178596,10.085256));
            latlon.add(109, new Point(-5.499455,10.016412));
            latlon.add(110, new Point(-5.647915,10.288608));
            latlon.add(111, new Point(-7.242339,8.223869));
            latlon.add(112, new Point(-7.642906,7.987517));
            latlon.add(113, new Point(-7.401403,8.526491));
            latlon.add(114, new Point(-7.776136,8.902044));
            latlon.add(115, new Point(-5.871374,5.312645));
            latlon.add(116, new Point(-6.190251,5.218782));
            latlon.add(117, new Point(-6.516798,5.909302));
            latlon.add(118, new Point(-6.375626,5.687274));
            latlon.add(119, new Point(-7.421456,4.591678));
            latlon.add(120, new Point(-7.290662,5.209427));
            latlon.add(121, new Point(-7.030853,4.860817));
            latlon.add(122, new Point(-6.672368,5.087328));
            latlon.add(123, new Point(-6.774255,5.633484));
            latlon.add(124, new Point(-6.953711,6.186966));
            latlon.add(125, new Point(-6.096103,5.585409));
            latlon.add(126, new Point(-6.583677,6.143608));
            latlon.add(127, new Point(-7.053202,10.102703));
            latlon.add(128, new Point(-7.509028,10.248889));
            latlon.add(129, new Point(-7.270937,9.036604));
            latlon.add(130, new Point(-7.575833,9.095466));
            latlon.add(131, new Point(-7.613464,9.888806));
            latlon.add(132, new Point(-7.486918,9.874800));
            latlon.add(133, new Point(-6.998730,9.679207));
            latlon.add(134, new Point(-7.329525,9.525759));
            latlon.add(135, new Point(-7.036917,9.255995));
            latlon.add(136, new Point(-7.942976,9.508245));
            latlon.add(137, new Point(-5.924879,5.872124));
            latlon.add(138, new Point(-6.020944,6.013225));
            latlon.add(139, new Point(-5.900705,6.291553));
            latlon.add(140, new Point(-6.246480,6.173249));
            latlon.add(141, new Point(-5.954574,6.447110));
            latlon.add(142, new Point(-5.620666,6.279958));
            latlon.add(143, new Point(-5.465715,6.442563));
            latlon.add(144, new Point(-6.439960,6.914402));
            latlon.add(145, new Point(-6.593350,6.803121));
            latlon.add(146, new Point(-6.868204,6.847144));
            latlon.add(147, new Point(-6.689813,6.998953));
            latlon.add(148, new Point(-6.234244,7.109681));
            latlon.add(149, new Point(-6.248125,6.716154));
            latlon.add(150, new Point(-6.607035,6.419355));
            latlon.add(151, new Point(-6.286847,6.492885));
            latlon.add(152, new Point(-6.493940,6.639638));
            latlon.add(153, new Point(-6.859862,6.521636));
            latlon.add(154, new Point(-6.511205,7.506542));
            latlon.add(155, new Point(-6.832057,7.529964));
            latlon.add(156, new Point(-6.903879,7.207536));
            latlon.add(157, new Point(-8.406591,6.499877));
            latlon.add(158, new Point(-7.185864,6.487173));
            latlon.add(159, new Point(-7.375065,6.012926));
            latlon.add(160, new Point(-8.017432,6.587344));
            latlon.add(161, new Point(-8.517804,6.510572));
            latlon.add(162, new Point(-8.432213,6.589037));
            latlon.add(163, new Point(-8.300833,6.486541));
            latlon.add(164, new Point(-7.190068,6.880202));
            latlon.add(165, new Point(-7.391132,6.723418));
            latlon.add(166, new Point(-7.106862,6.700423));
            latlon.add(167, new Point(-3.493922,6.664720));
            latlon.add(168, new Point(-3.247010,7.140284));
            latlon.add(169, new Point(-3.314225,6.673956));
            latlon.add(170, new Point(-4.474721,6.349143));
            latlon.add(171, new Point(-4.330981,6.436791));
            latlon.add(172, new Point(-4.215866,6.706639));
            latlon.add(173, new Point(-4.740074,6.765964));
            latlon.add(174, new Point(-3.911093,7.093113));
            latlon.add(175, new Point(-4.428258,7.010485));
            latlon.add(176, new Point(-3.706524,7.325061));
            latlon.add(177, new Point(-3.794398,7.521328));
            latlon.add(178, new Point(-4.348232,7.554560));
            latlon.add(179, new Point(-3.998962,7.748090));
            latlon.add(180, new Point(-4.251381,7.742958));
            latlon.add(181, new Point(-4.645248,7.294736));
            latlon.add(182, new Point(-5.251963,5.855107));
            latlon.add(183, new Point(-5.283333,5.542426));
            latlon.add(184, new Point(-5.252395,5.302369));
            latlon.add(185, new Point(-5.640117,5.324263));
            latlon.add(186, new Point(-5.231252,6.136609));
            latlon.add(187, new Point(-5.657858,5.837364));
            latlon.add(188, new Point(-5.876765,5.667795));
            latlon.add(189, new Point(-5.646757,6.101505));
            latlon.add(190, new Point(-3.281498,5.460473));
            latlon.add(191, new Point(-3.176211,5.703017));
            latlon.add(192, new Point(-3.526649,5.353724));
            latlon.add(193, new Point(-3.162765,5.995201));
            latlon.add(194, new Point(-2.960406,5.454036));
            latlon.add(195, new Point(-3.182704,5.197232));
            latlon.add(196, new Point(-2.886163,5.229328));
            latlon.add(197, new Point(-3.322690,5.270399));
            latlon.add(198, new Point(-3.768528,5.238152));
            latlon.add(199, new Point(-6.735694,8.845928));
            latlon.add(200, new Point(-6.617047,8.052000));
            latlon.add(201, new Point(-6.447620,8.211287));
            latlon.add(202, new Point(-6.957015,7.926330));
            latlon.add(203, new Point(-6.389606,7.904465));
            latlon.add(204, new Point(-6.934305,8.319946));
            latlon.add(205, new Point(-6.559708,8.529738));
            latlon.add(206, new Point(-7.031989,8.765110));
            latlon.add(207, new Point(-5.839227,7.852054));
            latlon.add(208, new Point(-6.159417,8.434637));
            latlon.add(209, new Point(-5.682539,8.207216));
            latlon.add(210, new Point(-6.062438,7.804191));
            latlon.add(211, new Point(-6.336081,8.834934));
            latlon.add(212, new Point(-2.814230,8.038299));
            latlon.add(213, new Point(-2.956078,7.955914));
            latlon.add(214, new Point(-3.085381,8.076264));
            latlon.add(215, new Point(-3.332398,8.175572));
            latlon.add(216, new Point(-3.085807,8.390748));
            latlon.add(217, new Point(-2.702508,8.430454));
            latlon.add(218, new Point(-3.733849,8.097782));
            latlon.add(219, new Point(-3.261930,9.747313));
            latlon.add(220, new Point(-3.371306,9.152394));
            latlon.add(221, new Point(-3.803905,9.748247));
            latlon.add(222, new Point(-3.559124,8.485728));
            latlon.add(223, new Point(-3.337320,7.514109));
            latlon.add(224, new Point(-3.589324,7.744112));
            latlon.add(225, new Point(-3.164144,7.277776));
            latlon.add(226, new Point(-3.054466,7.506552));
            latlon.add(227, new Point(-2.968563,7.718578));
            latlon.add(228, new Point(-3.209595,7.831601));
            latlon.add(229, new Point(-6.110841,8.059679));
            latlon.add(230, new Point(-5.848481,8.459074));
            latlon.add(231, new Point(-3.375685,5.163316));
            latlon.add(232, new Point(-4.202967,7.299683));
            latlon.add(233, new Point(-3.925581,6.769287));
            latlon.add(234, new Point(-4.611954,6.456701));
            latlon.add(235, new Point(-3.325625,6.095182));
            latlon.add(236, new Point(-7.464631,6.892702));
            latlon.add(237, new Point(-7.618685,6.482504));
            latlon.add(238, new Point(-7.844539,10.053335));
            latlon.add(239, new Point(-8.086464,9.577165));
            latlon.add(240, new Point(-7.713328,9.632629));
            latlon.add(241, new Point(-7.582452,8.674356));
            latlon.add(242, new Point(-7.556615,8.252904));
            latlon.add(243, new Point(-7.845864,8.191770));
            latlon.add(244, new Point(-8.069580,8.349665));
            latlon.add(245, new Point(-5.520300,9.417354));
            latlon.add(246, new Point(-5.767401,9.231673));
            latlon.add(247, new Point(-7.788882,6.950375));
            latlon.add(248, new Point(-7.555423,7.785384));
            latlon.add(249, new Point(-4.804022,7.449470));
            latlon.add(250, new Point(-4.968035,6.544904));
            latlon.add(251, new Point(-4.856789,6.347227));
            latlon.add(252, new Point(-5.225980,8.692048));
            latlon.add(253, new Point(-5.150988,8.095430));
            latlon.add(254, new Point(-5.861952,7.500366));
            latlon.add(255, new Point(-4.248280,5.807725));
        return latlon;
    }
     
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
                    String borra = "";
                    
                    // Data structure to save time stamps
                    ArrayList<Tupla> trace_times = new ArrayList<Tupla>();
                    double distance = 0.0;
                    while (iterator.hasNext()){
                        Tuple items = (Tuple) iterator.next();
                        DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy kk:mm:ss");
                        Date date = formatter.parse(String.format("%s/%s/%s %s:%s:%s", 
                                            items.get(3).toString().substring(5, 7),
                                            items.get(3).toString().substring(8, 10),
                                            items.get(3).toString().substring(0, 4),
                                            items.get(3).toString().substring(11, 13),
                                            items.get(3).toString().substring(14, 16),
                                            items.get(3).toString().substring(17, 18)));
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime(date);
                        
                        // Add trace
                        trace_times.add(new Tupla(calendar,items.get(2).toString()));
                        
                        // Sort traces by date
                        Collections.sort(trace_times, new DateComparator());
                        
                        // Load lat-lon
                        ArrayList<Point> latlon = getLatLon();
                        
                        int position = 0;
                        while (position < trace_times.size() - 1){
                            Tupla t0 = trace_times.get(position);
                            Tupla t1 = trace_times.get(position + 1);
                            if (!t0.subprefecture.equals("-1") && !t1.subprefecture.equals("-1")){
                                Point pt0 = latlon.get(Integer.parseInt(t0.subprefecture));
                                Point pt1 = latlon.get(Integer.parseInt(t1.subprefecture));
                                distance += pt0.get_euclidean_distance(pt1);
                            }
                            position += 1;
                        }
                        
                    }
                    // Sort array list to calculate distance traveled sorted by time 
                    
                    
                    // Add items to output
                    Tuple items = TupleFactory.getInstance().newTuple(2);

                     items.set(0, Double.toString(distance));
                     items.set(1, Integer.toString(trace_times.size()));
                     
                    output_databag.add(items);
                    
                } catch (Exception e){
                    Tuple items = TupleFactory.getInstance().newTuple(1);
                    
                    StringBuilder sb = new StringBuilder();
                    for (StackTraceElement element : e.getStackTrace()) {
                        sb.append(element.toString());
                        sb.append("\n");
                    }

                    items.set(0, "petting #1"+sb.toString()+"\n"+e.getMessage());
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
