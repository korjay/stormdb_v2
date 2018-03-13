package Air;

import AirMap.AirMap;
import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by honey on 17. 11. 13.
 */
public class no_step1_Bolt extends BaseRichBolt {
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
            }
        } catch (Exception e) {
            System.out.println("No2Bolt Exception : " + e.toString());
        }
    }

    public void execute(Tuple tuple) {
        double beginTime = System.currentTimeMillis();
        System.out.print("@@@NO Step 1 execute Start @@@");

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

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
