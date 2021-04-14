package gr.kwtsos;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import javafx.application.Platform;
import javafx.scene.chart.XYChart;

public class AverageBolt extends BaseBasicBolt {
    static final long serialVersionUID = 1;
    private int sum = 0;
    private int num = 0;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        ++num;
        sum += tuple.getInteger(0);
        double avg = ((double)sum) / num;
        new Thread(new Runnable() {
            @Override public void run() {
                Platform.runLater(new Runnable() {
                    @Override public void run() {
                        App.dataArea.appendText(String.format("%.3f", avg) + "\n");
                        App.sr.getData().add(new XYChart.Data<Number, Number>(num/5, avg));
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