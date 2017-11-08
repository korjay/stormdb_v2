package Air;

import AirMap.AirMap;
import AirMap_so2.AirMap_so2;
import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWException;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by honey on 17. 11. 3.
 */


public class AirSpout_so extends BaseRichSpout {
    private SpoutOutputCollector collector;
    static private Obj_so so_ground = null;
    static private Obj_so step2_data = null;
    static private Obj_so so_step2_data = null;
    static private MWNumericArray n = null;
    static private MWNumericArray region_n = null;
    static private MWNumericArray th = null;
    Object[] so_result_step1_1 = null;
    Object[] so_result_step1_2 = null;
    Object[] so_result_step2 = null;
    Object[] so_result_step2_1 = null;
    Object[] so_result_step2_2 = null;
    Object[] so_result_step2_3 = null;
    Object[] so_result_step1_3 = null;
    Object[] bld3d = null;
    AirMap_so2 airMap_so2;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;

        try {
            so_ground = new Obj_so();
            so_step2_data = new Obj_so();
            airMap_so2 = new AirMap_so2();
            for (int o = 0; o <= 1; o++) {
                region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
                th = new MWNumericArray(Double.valueOf(0.1),MWClassID.DOUBLE);
                so_result_step1_1 = airMap_so2.so_step1_1(5);
                so_result_step1_2 = airMap_so2.so_step1_2(5);
                so_result_step2 = airMap_so2.so_step2(1, region_n);
//                System.out.println("@#@#!@#!@#!so_result_step2@#@#!@#!@#! "+so_result_step2[0]);
                so_result_step2_1 = airMap_so2.so_step2_1(1, so_result_step2[0], n, region_n);
                so_result_step2_2 = airMap_so2.so_step2_2(2, so_result_step2_1[0], 0.1, so_result_step2[0]);
                so_result_step2_3 = airMap_so2.so_step2_3(5, so_result_step1_2[0], so_result_step1_2[3], so_result_step2[0]
                        , so_result_step2_2[0], n, region_n);
            }
        } catch (Exception e) {
            System.out.println("Air Spout_so Exception :" + e.toString());
        }
    }

    public void nextTuple() {
        double beginTime = System.currentTimeMillis();
        try {
            for (int i = 0; i <= 1; i++) {
                region_n = new MWNumericArray(Double.valueOf(i), MWClassID.DOUBLE);
                so_result_step1_1 = airMap_so2.so_step1_1(5);
                so_result_step1_2 = airMap_so2.so_step1_2(5);
                //ground data emit
                so_ground.setValue(so_result_step1_2);
                so_ground.setFlag(1);
                so_ground.setNum(i);
                this.collector.emit(new Values(so_ground.getValue(), so_ground.getFlag(), so_ground.getNum()));
            }

            for (int o = 1; o <= 51; o++) {
                n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
                so_result_step2_1 = airMap_so2.so_step2_1(1, so_result_step2[0], n, region_n);
                so_step2_data.setValue(so_result_step2_1);
                so_step2_data.setFlag(2);
                so_step2_data.setNum(o);
                this.collector.emit(new Values(so_step2_data.getValue(), so_step2_data.getFlag(), so_step2_data.getNum()));
                System.out.println("########## SO2 data emit##########");
                Thread.sleep(20 * 1);
            }
            Thread.sleep(1000 * 300);
        } catch (InterruptedException e) {
            System.out.println("@@@Spout Exception: " + e.toString());
        } catch (MWException e) {
            e.printStackTrace();
        }
        double endTime = System.currentTimeMillis();
        System.out
                .println("------------------------------------------------------");
        System.out.println("#####Making the SO AirSpout took " + (endTime - beginTime) / 1000
                + " seconds.#####");
        System.out
                .println("------------------------------------------------------");
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("so_ground_data", "so_step", "so_num"));
    }
}
