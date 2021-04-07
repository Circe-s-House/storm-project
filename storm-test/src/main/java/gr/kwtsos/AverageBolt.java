package gr.kwtsos;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class AverageBolt extends BaseBasicBolt {
    static final long serialVersionUID = 1;
    private int sum = 0;
    private int num = 0;
    private BufferedWriter fileWriter;

    @Override
    public void prepare(Map<String,Object> topoConf, TopologyContext context) {
        try {
            fileWriter = new BufferedWriter(new FileWriter("results.txt"));
        } catch (IOException ioe) {}
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        ++num;
        sum += tuple.getInteger(0);
        double avg = ((double)sum) / num;
        try {
            fileWriter.write(Double.toString(avg) + "\n");
            fileWriter.flush();
        } catch (IOException ioe) {}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("avg"));
    }
}