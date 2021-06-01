package storm_project;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.commons.lang3.tuple.ImmutableTriple;
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
                            "%s,%s,%s,%d,%d,%d\n",
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
                            Tuple old = App.k24Map.getOrDefault(unixTime, tuple);
                            App.k24Map.put(unixTime, tuple);
                            int deltaT = temperature - old.getIntegerByField("temperature");
                            int deltaK = knots - old.getIntegerByField("knots");
                            int deltaH = humidity - old.getIntegerByField("humidity");
                            if (deltaT != 0 || deltaK != 0 || deltaH != 0) {
                                App.k24Delta.put(unixTime, new ImmutableTriple<Integer, Integer, Integer>(
                                    deltaT, deltaK, deltaH
                                ));
                            }
                        } else if (site.equals("meteo.gr")) {
                            Tuple old = App.meteoMap.getOrDefault(unixTime, tuple);
                            App.meteoMap.put(unixTime, tuple);
                            int deltaT = temperature - old.getIntegerByField("temperature");
                            int deltaK = knots - old.getIntegerByField("knots");
                            int deltaH = humidity - old.getIntegerByField("humidity");
                            if (deltaT != 0 || deltaK != 0 || deltaH != 0) {
                                App.meteoDelta.put(unixTime, new ImmutableTriple<Integer, Integer, Integer>(
                                    deltaT, deltaK, deltaH
                                ));
                            }
                        } else if (site.equals("okairos.gr")) {
                            Tuple old = App.okairosMap.getOrDefault(unixTime, tuple);
                            App.okairosMap.put(unixTime, tuple);
                            int deltaT = temperature - old.getIntegerByField("temperature");
                            int deltaK = knots - old.getIntegerByField("knots");
                            int deltaH = humidity - old.getIntegerByField("humidity");
                            if (deltaT != 0 || deltaK != 0 || deltaH != 0) {
                                App.okairosDelta.put(unixTime, new ImmutableTriple<Integer, Integer, Integer>(
                                    deltaT, deltaK, deltaH
                                ));
                            }
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