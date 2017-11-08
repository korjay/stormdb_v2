package Air;

import AirMap.AirMap;
import AirMap_so2.AirMap_so2;
import com.mathworks.toolbox.javabuilder.MWClassID;
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
public class So2Bolt extends BaseRichBolt {
    private OutputCollector collector;

    static private MWNumericArray n = null;
    static private MWNumericArray th = null;
    static private MWNumericArray region_n = null;
    static private Object[] so_result_step1_1 = null;
    static private Object[] so_result_step1_2 = null;
    static private Object[] so_result_step1_3 = null;
    static private Object[] so_result_step1_4 = null;
    static private Object[] so_result_step2 = null;
    static private Object[] so_result_step2_1 = null;
    static private Object[] so_result_step2_2 = null;
    static private Object[] so_result_step2_3 = null;
    static private Object[] so_result_step2_4 = null;
    static private Object[] so_result_step3 = null;
    static private Object[] bld3d = null;
    static private AirMap_so2 airMap_so2 = null;
    static private Obj_so so_ground = null;
    static private Obj_so step2_region = null;
    static private Obj_so so_step2_data = null;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        try {
            so_ground = new Obj_so();
            step2_region = new Obj_so();
            so_step2_data = new Obj_so();

            for (int o = 0; o <= 1; o++) {
                System.out.println("###SO step2 region_n ###= " + o);
                region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
                so_result_step1_1 = airMap_so2.so_step1_1(5);
                so_result_step1_2 = airMap_so2.so_step1_2(5);
                so_result_step2 = airMap_so2.so_step2(1, region_n);
//                result_step2 = airMap.step2(1,region_n);
                so_result_step2_1 = airMap_so2.so_step2_1(1, so_result_step2[0], region_n);
                so_result_step2_3 = airMap_so2.so_step2_3(5,so_result_step1_2[0], so_result_step1_2[3], so_result_step2[0],
                        so_result_step2_2[0], n, region_n);
            }
        } catch (Exception e) {
            System.out.println("So2Bolt Exception : " + e.toString());
        }
    }

    public void execute(Tuple tuple) {
        double beginTime = System.currentTimeMillis();
        try{
            so_step2_data.setValue((Object[])tuple.getValueByField("so_ground_data"));
            so_step2_data.setFlag((Integer)tuple.getValueByField("so_step"));
            so_step2_data.setNum((Integer)tuple.getValueByField("so_num"));
            System.out.print("@@@SO Step 2 Start @@@");

            if(so_ground.getFlag() == 1){
                so_result_step1_2 = so_ground.getValue();
                System.out.println("###########SO2 ground value = " + so_ground.getValue());
                this.airMap_so2 = new AirMap_so2();

                try {
                    int o = so_ground.getNum();
                    region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
                    so_result_step1_3 = airMap_so2.so_step1_3(1, so_result_step1_2[1], so_result_step1_2[3], so_result_step1_2[4],
                            region_n);
                    so_result_step1_4 = airMap_so2.so_step1_4(so_result_step1_3[0], so_result_step1_2[3], region_n);

                }
                catch (Exception e){
                    System.out.println("Exception : " + e.toString());
                }
            }

            try{
                if(so_step2_data.getFlag() == 2) {
                    int i = so_step2_data.getNum();
                    so_result_step2_1 = so_step2_data.getValue();
                    int o = step2_region.getNum();
                    region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
                    n = new MWNumericArray(Double.valueOf(i), MWClassID.DOUBLE);
                    so_result_step2 = airMap_so2.so_step2(1, region_n);
                    so_result_step2_2 = airMap_so2.so_step2_2(2, so_result_step2_1[0], 0.1, so_result_step2[0]);
                    so_result_step2_3 = airMap_so2.so_step2_3(5, so_result_step1_2[0], so_result_step1_2[3], so_result_step2[0]
                            , so_result_step2_2[0], n, region_n);
                    so_result_step2_4 = airMap_so2.so_step2_4(so_result_step2_3[0], so_result_step2_3[1], so_result_step2_3[2],
                            so_result_step2_3[3], so_result_step2_3[4], region_n, so_result_step2[0]);
                    so_result_step3 = airMap_so2.so_step3(so_result_step1_1[0], so_result_step1_1[1], so_result_step1_1[2],
                            so_result_step1_1[4], so_result_step2[0], region_n);
                }
            }catch (Exception e){
                System.out.println("SO step 2 Exception:" + e.toString());
            }

        }catch (Exception e){
            System.out.println("Spout input stream Exception : " + e.toString());
        }
        double endTime = System.currentTimeMillis();
        System.out
                .println("------------------------------------------------------");
        System.out.println("#####Making the SO Step2Bolt took " + (endTime - beginTime) / 1000
                + " seconds.#####");
        System.out
                .println("------------------------------------------------------");


    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
