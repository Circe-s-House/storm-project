package gr.kwtsos;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.InputDeclarer;
import org.apache.storm.topology.TopologyBuilder;

public class App {
    public static void main(String[] args){

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("IntegerSpout", new CSVSpout());
        InputDeclarer declarer = builder.setBolt("MultiplierBolt", new MultiplierBolt());
        declarer.shuffleGrouping("IntegerSpout");

        Config config = new Config();
        //config.setDebug(true);

        LocalCluster cluster = null;
        try{
            cluster = new LocalCluster();
            cluster.submitTopology("HelloTopology", config, builder.createTopology());
            Thread.sleep(10000);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            cluster.shutdown();
        }
    }
}