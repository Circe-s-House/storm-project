package gr.kwtsos;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import com.opencsv.CSVReader;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class CSVSpout extends BaseRichSpout {
    SpoutOutputCollector spoutOutputCollector;
    private Integer index = 0;
    static final long serialVersionUID = 0;
    CSVReader csvReader;

    public void initialize(String path) throws FileNotFoundException, IOException {
        try (FileReader fileReader = new FileReader(path)) {
            csvReader = new CSVReader(fileReader);
        }
    }

    public void open(Map conf, TopologyContext TopologyContext, SpoutOutputCollector spoutOutputCollector){
        this.spoutOutputCollector = spoutOutputCollector;
    }

    public void nextTuple(){
        if (index < 100) {
            this.spoutOutputCollector.emit(new Values(index));
            index++;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
        outputFieldsDeclarer.declare(new Fields("field"));
    }
}