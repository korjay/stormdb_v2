package Air;

import AirMap.AirMap;
import AirMap_so2.AirMap_so2;
import com.mathworks.toolbox.javabuilder.MWNumericArray;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by honey on 17. 10. 22.
 */
public class AirTopology {
    static public MWNumericArray n = null;
    static public MWNumericArray region_n = null;
    static public Object[] result_step1_1 = null;
    static public Object[] result_step1_2 = null;
    static public Object[] bld3d = null;
    private static AirMap airMap = null;
    private static AirMap_so2 airMap_so2 = null;


    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {
        double beginTime = System.currentTimeMillis();
        Config conf = new Config();

        System.out.println("@@@ Create Topology Builder @@@");
        //make Topology
        TopologyBuilder builder = new TopologyBuilder();
        //make kafka-spout
//        builder.setSpout("kafka_spout", new kafka_so_spout(), 1);
//        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 2048);
//        conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, false);
//        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
//        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
        //make Spout
        builder.setSpout("spout", new AirSpout(), 1);
        //make so2Spout
        builder.setSpout("spout_so", new AirSpout_so(), 1);
        //make No2Bolt
        builder.setBolt("no2", new No2Bolt(), 30)
                .shuffleGrouping("spout");
        //make So2Bolt
        builder.setBolt("so2", new So2Bolt(), 30)
                .shuffleGrouping("spout_so");


//        //Local mode
//        LocalCluster cluster = new LocalCluster();
//        Config config = new Config();
//        cluster.submitTopology("AirTopology", config, builder.createTopology());
//
//        try {
//            Thread.sleep(1000 * 660);
//        } catch (InterruptedException e) {
//
//        }
//        cluster.killTopology("AirTopology");
//        cluster.shutdown();

//        Config conf = new Config();
        conf.setNumAckers(0);
        conf.setNumWorkers(40);
        conf.setMaxSpoutPending(5000);
        StormSubmitter.submitTopology("AirTopology",conf,builder.createTopology());
        double endTime = System.currentTimeMillis();
        System.out
                .println("------------------------------------------------------");
        System.out.println("#####Making the AirTopology took " + (endTime - beginTime) / 1000
                + " seconds.#####");
        System.out
                .println("------------------------------------------------------");
        Thread.sleep(1000*50);
    }


}



