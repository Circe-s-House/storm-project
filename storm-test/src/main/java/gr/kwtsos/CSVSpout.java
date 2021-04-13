package gr.kwtsos;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class CSVSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    static final long serialVersionUID = 0;
    private final String filePath;
    private BufferedReader fileReader;
    private CSVParser csvParser;

    public CSVSpout(String path) {
        filePath = path;
    }

    public void open(Map conf, TopologyContext TopologyContext, SpoutOutputCollector spoutOutputCollector){
        this.spoutOutputCollector = spoutOutputCollector;
        csvParser = new CSVParserBuilder().withSeparator(';').build();
        try {
            fileReader = new BufferedReader(new FileReader(filePath));
        } catch (IOException e) {};
    }

    public void nextTuple() {
        int result = 0;
        try {
            TimeUnit.MILLISECONDS.sleep(200);
        } catch (InterruptedException ie) {}
        try {
            String[] row = csvParser.parseLine(fileReader.readLine());
            result = Integer.parseInt(row[12]);
            this.spoutOutputCollector.emit(new Values(result));
        } catch (Exception e) {

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("WINDFORCEKNOT"));
    }
}