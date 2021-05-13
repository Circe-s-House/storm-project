package storm_project;

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
        csvParser = new CSVParserBuilder().withSeparator(',').build();
        try {
            fileReader = new BufferedReader(new FileReader(filePath));
        } catch (IOException e) {};
    }

    public void nextTuple() {
        try {
            TimeUnit.MILLISECONDS.sleep(1000);
        } catch (InterruptedException ie) {}
        try {
            String[] row = csvParser.parseLine(fileReader.readLine());
            String site = row[0];
            String date = row[1];
            String time = row[2];
            int temperature = Integer.parseInt(row[3]);
            int knots = Integer.parseInt(row[4]);
            int humidity = Integer.parseInt(row[5]);
            this.spoutOutputCollector.emit(new Values(
                site, date, time, temperature, knots, humidity));
        } catch (Exception e) {}
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
            "site", "date", "time", "temperature", "knots", "humidity"));
    }
}