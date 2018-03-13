import Air.ObjectArray;
import AirMap.AirMap;
import com.mathworks.toolbox.javabuilder.MWClassID;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by honey on 17. 10. 23.
 */
public class AirSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    static private ObjectArray ground = null;
    static private ObjectArray step2_region = null;
    static private ObjectArray emit_data1 = null;
    static private ObjectArray so_emit_data1 = null;
    static private ObjectArray step2_data = null;
    static private ObjectArray so_step2_data = null;
    static private MWNumericArray n = null;
    static private MWNumericArray region_n = null;
    static private MWNumericArray th = null;
    Object[] result_step1_1 = null;
    Object[] result_step1_2 = null;
    Object[] result_step2 = null;
    Object[] result_step2_1 = null;
    Object[] result_step2_2 = null;
    Object[] result_step2_3 = null;
    Object[] bld3d = null;
    AirMap airMap;



    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        try {
            Thread.sleep(1000 * 30);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.collector = collector;

        try {
            ground = new ObjectArray();
            step2_data = new ObjectArray();
            step2_region = new ObjectArray();
            airMap = new AirMap();
            for (int o = 0; o <= 1; o++) {
                region_n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
                th = new MWNumericArray(Double.valueOf(0.1),MWClassID.DOUBLE);
                result_step1_1 = airMap.step1_1(5);
                result_step1_2 = airMap.step1_2(5);
                result_step2 = airMap.step2(1, region_n);
                result_step2_1 = airMap.step2_1(1, result_step2[0], n, region_n);
                result_step2_2 = airMap.step2_2(2, result_step2_1[0], 0.1, result_step2[0]);
                result_step2_3 = airMap.step2_3(5, result_step1_2[0], result_step1_2[3], result_step2[0]
                        , result_step2_2[0], n, region_n);

            }
        } catch (Exception e) {
            System.out.println("open Exception: " + e.toString());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
        outputFieldsDeclarer.declare(new Fields("ground_data", "step", "num"));
    }
    public void nextTuple(){
        double beginTime = System.currentTimeMillis();
        try {
            for(int i = 0; i <= 1; i++){
                region_n = new MWNumericArray(Double.valueOf(i), MWClassID.DOUBLE);
                System.out.println("@@@@NO2 spout region :"+region_n);
                result_step1_1 = airMap.step1_1(5);
                result_step1_2 = airMap.step1_2(5);
                //ground data emit
                ground.setValue(result_step1_2);
                ground.setFlag(1);
                ground.setNum(i);
                step2_region.setNum(i);
                this.collector.emit(new Values(ground.getValue(), ground.getFlag(), ground.getNum()));
                for(int o = 1; o <=51; o++){
                    n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
                    result_step2_1 = airMap.step2_1(1, result_step2[0], n, region_n);
                    step2_data.setValue(result_step2_1);
                    step2_data.setFlag(2);
                    step2_data.setNum(o);
                    this.collector.emit(new Values(step2_data.getValue(), step2_data.getFlag(), step2_data.getNum()));
                    System.out.println("########## no2 data emit##########");
                    System.out.println("NO2 stage :"+o);
                    Thread.sleep(500 * 1);
                }
            }

//            for(int o = 1; o <=51; o++){
//                n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
//                result_step2_1 = airMap.step2_1(1, result_step2[0], n, region_n);
//                step2_data.setValue(result_step2_1);
//                step2_data.setFlag(2);
//                step2_data.setNum(o);
//                this.collector.emit(new Values(step2_data.getValue(), step2_data.getFlag(), step2_data.getNum()));
//                System.out.println("########## no2 data emit##########");
//                Thread.sleep(20 * 1);
//            }
            Thread.sleep(1000 * 50);


        } catch (Exception e) {
            System.out.println("@@@Spout Exception: " + e.toString());
        }
//        try {
//            for(int i = 0; i <= 1; i++){
//                region_n = new MWNumericArray(Double.valueOf(i), MWClassID.DOUBLE);
//                so_result_step1_1 = airMap.so_step1_1(5);
//                so_result_step1_2 = airMap.so_step1_2(5);
//                //ground data emit
//                so_ground.setValue(result_step1_2);
//                so_ground.setFlag(1);
//                this.collector.emit(new Values(so_ground.getValue(), so_ground.getFlag(), so_ground.getNum()));
//            }
//
//            for(int o = 1; o <=51; o++){
//                n = new MWNumericArray(Double.valueOf(o), MWClassID.DOUBLE);
//                so_result_step2_1 = airMap.so_step2_1(1, so_result_step2[0], n, region_n);
//                so_step2_data.setValue(so_result_step2_1);
//                so_step2_data.setFlag(2);
//                so_step2_data.setNum(o);
//                this.collector.emit(new Values(so_step2_data.getValue(), so_step2_data.getFlag(), so_step2_data.getNum()));
//                System.out.println("########## so2 data emit##########");
//            }
//            Thread.sleep(60,30);
//
//        } catch (MWException e) {
//            System.out.println("@@@Spout Exception: " + e.toString());
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        double endTime = System.currentTimeMillis();
        System.out
                .println("------------------------------------------------------");
        System.out.println("#####Making the NO AirSpout took " + (endTime - beginTime) / 1000
                + " seconds.#####");
        System.out
                .println("------------------------------------------------------");
    }

}
