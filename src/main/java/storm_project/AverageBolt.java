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
    public static final Map<Long, Tuple> k24Table = makeMap(52);
    public static final Map<Long, Tuple> meteoTable = makeMap(44);
    public static final Map<Long, Tuple> okairosTable = makeMap(252 / 3);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        final String site = tuple.getStringByField("site");
        final String date = tuple.getStringByField("date");
        final String time = tuple.getStringByField("time");
        final int temperature = tuple.getIntegerByField("temperature");
        final int knots = tuple.getIntegerByField("knots");
        final int humidity = tuple.getIntegerByField("humidity");

        Date unixDate = null;
        try {
            System.out.println("========" + date + "/" + currentYear + " " + time + "========");
            unixDate = new SimpleDateFormat("dd/MM/yyyy HH:mm").parse(
                date + "/" + currentYear + " " + time);
        } catch (ParseException pe) {
            System.out.println("Could not parse date and time");
        }

        final long unixTime = unixDate.getTime();
        if (site.equals("k24.net")) {
            k24Table.put(unixTime, tuple);
        } else if (site.equals("meteo.gr")) {
            meteoTable.put(unixTime, tuple);
        } else if (site.equals("okairos.gr")) {
            okairosTable.put(unixTime, tuple);
        }

        new Thread(new Runnable() {
            @Override public void run() {
                Platform.runLater(new Runnable() {
                    @Override public void run() {
                        App.dataArea.appendText(String.format(
                            "%s, %s, %s, %d, %d, %d\n",
                            site, date, time, temperature, knots, humidity));
                    }
                });
            }
        }).start();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("avg"));
    }

    public static Map<Long, Tuple> makeMap(int size) {
        return Collections.synchronizedMap(
            new LinkedHashMap<Long, Tuple>() {
                @Override
                protected boolean removeEldestEntry(final Map.Entry<Long, Tuple> eldest) {
                    return size() > size;
                }
            }
        );
    }
}