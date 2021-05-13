package storm_project;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import javafx.application.Platform;

public class AverageBolt extends BaseBasicBolt {
    static final long serialVersionUID = 1;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String site = tuple.getStringByField("site");
        int temperature = tuple.getIntegerByField("temperature");
        int humidity = tuple.getIntegerByField("humidity");
        new Thread(new Runnable() {
            @Override public void run() {
                Platform.runLater(new Runnable() {
                    @Override public void run() {
                        App.dataArea.appendText(String.format("%s, %d, %d\n", site, temperature, humidity));
                    }
                });
            }
        }).start();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("avg"));
    }
}
