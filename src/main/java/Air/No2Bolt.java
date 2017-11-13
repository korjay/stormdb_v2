package Air;

import AirMap.AirMap;
import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWException;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by honey on 17. 10. 23.
 */
public class No2Bolt extends BaseRichBolt {
    private OutputCollector collector;

    static private MWNumericArray n = null;
    static private MWNumericArray th = null;
    static private MWNumericArray region_n = null;
    static private ObjectArray step1_data = null;
    static private Object[] result_step1_1 = null;
    static private Object[] result_step1_2 = null;
    static private Object[] result_step1_3 = null;
    static private Object[] result_step1_4 = null;
    static private Object[] result_step2 = null;
    static private Object[] result_step2_1 = null;
    static private Object[] result_step2_2 = null;
    static private Object[] result_step2_3 = null;
    static private Object[] result_step2_4 = null;
    static private Object[] result_step3 = null;
    static private Object[] bld3d = null;
    static private AirMap airMap = null;
    static private ObjectArray ground = null;
    static private ObjectArray step2_region = null;
    static private ObjectArray step2_data = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        try {
            ground = new ObjectArray();
            step2_region = new ObjectArray();
            step2_data = new ObjectArray();
            airMap = new AirMap();
                for (int o = 0; o <= 1; o++) {
                    System.out.println("###NO step2 region_n ###= " + o);
                    region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
                    result_step1_1 = airMap.step1_1(5);
                    result_step1_2 = airMap.step1_2(5);
                    result_step2 = airMap.step2(1, region_n);
//                result_step2 = airMap.step2(1,region_n);
                    result_step2_1 = airMap.step2_1(1, result_step2[0], region_n);
                    result_step2_3 = airMap.step2_3(5,result_step1_2[0], result_step1_2[3], result_step2[0],
                            result_step2_2[0], n, region_n);
                }
            } catch (Exception e) {
                System.out.println("No2Bolt Exception : " + e.toString());
            }
    }


    public void execute(Tuple tuple) {
        double beginTime = System.currentTimeMillis();
        try{
        step2_data.setValue((Object[])tuple.getValueByField("ground_data"));
        step2_data.setFlag((Integer)tuple.getValueByField("step"));
        step2_data.setNum((Integer)tuple.getValueByField("num"));
        System.out.print("@@@NO Step 2 execute Start @@@");


        if(ground.getFlag() == 1){
            result_step1_2 = ground.getValue();
            System.out.println("###########NO ground value = " + ground.getValue());

            try {
                ground = new ObjectArray();
                int o = ground.getNum();
                region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
                result_step1_3 = airMap.step1_3(1, result_step1_2[1], result_step1_2[3], result_step1_2[4],
                        region_n);
                result_step1_4 = airMap.step1_4(result_step1_3[0], result_step1_2[3], region_n);

            }
            catch (Exception e){
                System.out.println("Exception : " + e.toString());
            }
        }

        try{
        if(step2_data.getFlag() == 2) {
            int i = step2_data.getNum();
            result_step2_1 = step2_data.getValue();

            n = new MWNumericArray(Double.valueOf(i), MWClassID.DOUBLE);
            int o = step2_region.getNum();
            System.out.println("NO2 step 2 stage :" + o);
            region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
            result_step2 = airMap.step2(1, region_n);
            result_step2_2 = airMap.step2_2(2, result_step2_1[0], 0.1, result_step2[0]);
            result_step2_3 = airMap.step2_3(5, result_step1_2[0], result_step1_2[3], result_step2[0]
                    , result_step2_2[0], n, region_n);
            result_step2_4 = airMap.step2_4(result_step2_3[0], result_step2_3[1], result_step2_3[2],
                    result_step2_3[3], result_step2_3[4], region_n, result_step2[0]);
//            result_step3 = airMap.step3(result_step1_1[0], result_step1_1[1], result_step1_1[2],
//                    result_step1_1[4], result_step2[0], region_n);
        }
        }catch (Exception e){
            System.out.println("NO step 2 Exception:" + e.toString());
        }
//            result_step3 = airMap.step3(result_step1_1[0], result_step1_1[1], result_step1_1[2],
//                    result_step1_1[4], result_step2[0], region_n);
            result_step3 = airMap.step3(result_step1_1[0], result_step1_1[1], result_step1_1[2],
                    result_step1_1[4], result_step2[0], region_n);
        }catch (Exception e){
            System.out.println("Spout input stream Exception : " + e.toString());

        }


        double endTime = System.currentTimeMillis();
        System.out
                .println("------------------------------------------------------");
        System.out.println("#####Making the NO Step2Bolt took " + (endTime - beginTime) / 1000
                + " seconds.#####");
        System.out
                .println("------------------------------------------------------");

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
