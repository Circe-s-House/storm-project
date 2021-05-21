package storm_project;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import javafx.application.Platform;

public class AverageBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1;
    private static final int currentYear = new GregorianCalendar().get(Calendar.YEAR);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        final String site = tuple.getStringByField("site");
        final String date = tuple.getStringByField("date");
        final String time = tuple.getStringByField("time");
        final int temperature = tuple.getIntegerByField("temperature");
        final int knots = tuple.getIntegerByField("knots");
        final int humidity = tuple.getIntegerByField("humidity");

        new Thread(new Runnable() {
            @Override public void run() {
                Platform.runLater(new Runnable() {
                    @Override public void run() {
                        App.dataArea.appendText(String.format(
                            "%s, %s, %s, %d, %d, %d\n",
                            site, date, time, temperature, knots, humidity));

                        Date unixDate = null;
                        try {
                            unixDate = new SimpleDateFormat("dd/MM/yyyy HH:mm").parse(
                                date + "/" + currentYear + " " + time);
                        } catch (ParseException pe) {
                            System.out.println("Could not parse date and time");
                        }
                        final long unixTime = unixDate.getTime();
                        if (site.equals("k24.net")) {
                            App.k24Map.put(unixTime, tuple);
                        } else if (site.equals("meteo.gr")) {
                            App.meteoMap.put(unixTime, tuple);
                        } else if (site.equals("okairos.gr")) {
                            App.okairosMap.put(unixTime, tuple);
                        }
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