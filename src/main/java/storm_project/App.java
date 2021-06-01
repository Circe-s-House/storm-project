package storm_project;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.InputDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;

import javafx.application.Application;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.MapChangeListener;
import javafx.collections.ObservableMap;
import javafx.concurrent.Task;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.RadioButton;
import javafx.scene.control.Spinner;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.ToggleGroup;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

public class App extends Application {
    public static TextArea dataArea = new TextArea();
    public static TextArea msgArea = new TextArea();
    public static Map<Long, Tuple> k24Map = makeTupleMap(1000);
    public static Map<Long, Tuple> meteoMap = makeTupleMap(1000);
    public static Map<Long, Tuple> okairosMap = makeTupleMap(1000);
    public static ObservableMap<Long, Triple<Integer, Integer, Integer>> k24Delta = makeDeltaMap(1000);
    public static ObservableMap<Long, Triple<Integer, Integer, Integer>> meteoDelta = makeDeltaMap(1000);
    public static ObservableMap<Long, Triple<Integer, Integer, Integer>> okairosDelta = makeDeltaMap(1000);
    private static TableView<List<String>> table = new TableView<>();
    private static RadioButton rbAbsolute = new RadioButton("absolute  ");
    private static Spinner<Integer> spAbsolute = new Spinner<>(0, 100, 0, 1);
    private static RadioButton rbPercentage = new RadioButton("percentage");
    private static Spinner<Integer> spPercentage = new Spinner<>(0, 100, 0, 1);
    private static RadioButton rbc = new RadioButton("°C  ");
    private static RadioButton rbk = new RadioButton("kn  ");
    private static RadioButton rbh = new RadioButton("%hmd");
    public static void main(String[] args) throws FileNotFoundException, IOException, TException, Exception {
        TopologyBuilder builder = new TopologyBuilder();
        runCmd("rm -rf data");
        runCmd("mkdir data");
        runCmd("touch data/k24.csv data/meteo.csv data/okairos.csv");
        builder.setSpout("k24Spout", new CSVSpout("data/k24.csv"));
        builder.setSpout("meteoSpout", new CSVSpout("data/meteo.csv"));
        builder.setSpout("okairosSpout", new CSVSpout("data/okairos.csv"));
        InputDeclarer declarer = builder.setBolt("AverageBolt", new AverageBolt());
        declarer.shuffleGrouping("meteoSpout");
        declarer.shuffleGrouping("okairosSpout");
        declarer.shuffleGrouping("k24Spout");

        Config conf = new Config();
        conf.setDebug(false);

        try (LocalCluster cluster = new LocalCluster()) {
            cluster.submitTopology("HelloTopology", conf, builder.createTopology());
            launch(args);
            Thread.sleep(10000);
        }
    }

    @Override
    public void start(Stage stage) {
        BorderPane mainPane = new BorderPane();

        dataArea.setPrefWidth(315);
        Button but1 = new Button("k24");
        but1.setMinWidth(105);
        but1.setMinHeight(20);
        Button but2 = new Button("meteo");
        but2.setMinWidth(105);
        but2.setMinHeight(20);
        Button but3 = new Button("okairos");
        but3.setMinWidth(105);
        but3.setMinHeight(20);
        HBox buttonPane = new HBox();
        buttonPane.getChildren().addAll(but1, but2, but3);

        msgArea.setPrefWidth(315);
        msgArea.setPrefHeight(150);

        spAbsolute.valueProperty().addListener((obs, oldValue, newValue) -> {
            updateTable();
        });

        spPercentage.valueProperty().addListener((obs, oldValue, newValue) -> {
            updateTable();
        });

        HBox settingsBox1 = new HBox();
        settingsBox1.getChildren().addAll(rbAbsolute, spAbsolute);
        settingsBox1.setSpacing(5);

        HBox settingsBox2 = new HBox();
        settingsBox2.getChildren().addAll(rbPercentage, spPercentage);
        settingsBox2.setSpacing(5);

        ToggleGroup tg1 = new ToggleGroup();
        rbAbsolute.setToggleGroup(tg1);
        rbPercentage.setToggleGroup(tg1);
        tg1.selectedToggleProperty().addListener((o, oldVal, newVal) -> {
            updateTable();
        });

        VBox absPerBox = new VBox(settingsBox1, settingsBox2);
        absPerBox.setAlignment(Pos.CENTER);
        absPerBox.setSpacing(15);

        ToggleGroup tg2 = new ToggleGroup();
        rbc.setToggleGroup(tg2);
        rbk.setToggleGroup(tg2);
        rbh.setToggleGroup(tg2);
        tg2.selectedToggleProperty().addListener((o, oldVal, newVal) -> {
            updateTable();
        });

        VBox settingsBox3 = new VBox(rbc, rbk, rbh);
        settingsBox3.setAlignment(Pos.CENTER);
        settingsBox3.setSpacing(15);

        Button butRefresh = new Button("refresh");
        butRefresh.setOnAction((event) -> {
            updateTable();
            msgArea.appendText("Refreshed table\n");
        });

        HBox settingsBox = new HBox();
        settingsBox.getChildren().addAll(absPerBox, settingsBox3, butRefresh);
        settingsBox.setAlignment(Pos.CENTER);
        settingsBox.setSpacing(50);

        HBox botArea = new HBox(msgArea, settingsBox);
        botArea.setSpacing(20);

        mainPane.setTop(buttonPane);
        mainPane.setBottom(botArea);
        mainPane.setLeft(dataArea);

        table.setEditable(false);
        table.getColumns().add(makeColumn(0, "time"));
        table.getColumns().add(makeColumn(1, "filter"));
        table.getColumns().add(makeColumn(2, "k24 °C"));
        table.getColumns().add(makeColumn(3, "meteo °C"));
        table.getColumns().add(makeColumn(4, "okairos °C"));
        table.getColumns().add(makeColumn(5, "k24 kn"));
        table.getColumns().add(makeColumn(6, "meteo kn"));
        table.getColumns().add(makeColumn(7, "okairos kn"));
        table.getColumns().add(makeColumn(8, "k24 %hmd"));
        table.getColumns().add(makeColumn(9, "meteo %hmd"));
        table.getColumns().add(makeColumn(10, "okairos %hmd"));
        mainPane.setCenter(table);

        Scene scene = new Scene(mainPane, 1600, 900);
        scene.getStylesheets().add(getClass().getResource("style.css").toExternalForm());
        but1.setOnAction((event) -> { runSpider("k24"); });
        but2.setOnAction((event) -> { runSpider("meteo"); });
        but3.setOnAction((event) -> { runSpider("okairos"); });

        stage.setScene(scene);
        stage.show();

        spawnThread("k24", 3).start();
        try { TimeUnit.MILLISECONDS.sleep(50); } catch (InterruptedException e) {}
        spawnThread("meteo", 3).start();
        try { TimeUnit.MILLISECONDS.sleep(50); } catch (InterruptedException e) {}
        spawnThread("okairos", 1).start();
    }

    @Override
    public void stop() {
        try {
            Runtime.getRuntime().exec("rm -rf data");
        } catch (IOException e) {}
        System.exit(0);
    }

    public static void runCmd(String cmd) {
        try {
            Runtime.getRuntime().exec(cmd);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void runSpider(String site) {
        runCmd("scrapy runspider spiders/" + site + ".py -o data/" + site + ".csv");
        msgArea.appendText("Scraped " + site + "\n");
    }

    public static Thread spawnThread(String site, int delay) {
        return new Thread(new Task<Void>() {
            @Override public Void call() {
                while (true) {
                    runSpider(site);
                    try {
                        TimeUnit.HOURS.sleep(delay);
                    } catch (InterruptedException e) {}
                }
            };
        });
    }

    public static TableColumn<List<String>, String> makeColumn(int index, String label) {
        TableColumn<List<String>, String> col = new TableColumn<>(label);
        col.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().get(index)));
        col.setMinWidth(115);
        return col;
    }

    public static Map<Long, Tuple> makeTupleMap(int size) {
        return new LinkedHashMap<Long, Tuple>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<Long, Tuple> eldest) {
                return size() > size;
            }
        };
    }

    public static ObservableMap<Long, Triple<Integer, Integer, Integer>> makeDeltaMap(int size) {
        Map<Long, Triple<Integer, Integer, Integer>> map = Collections.synchronizedMap(
            new LinkedHashMap<Long, Triple<Integer, Integer, Integer>>() {
                @Override
                protected boolean removeEldestEntry(final Map.Entry<Long, Triple<Integer, Integer, Integer>> eldest) {
                    return size() > size;
                }
            }
        );
        ObservableMap<Long, Triple<Integer, Integer, Integer>> observableMap = FXCollections.observableMap(map);
        observableMap.addListener(new MapChangeListener<Long, Triple<Integer, Integer, Integer>>() {
            @Override
            public void onChanged(MapChangeListener.Change<? extends Long, ? extends Triple<Integer, Integer, Integer>> change) {
                if (change.wasAdded() || change.wasRemoved()) {
                    updateTable();
                }
            }
        });
        return observableMap;
    }

    public static void updateTable() {
        table.getItems().clear();
        for (Map.Entry<Long, Tuple> entry : meteoMap.entrySet()) {
            long key = entry.getKey();

            String datetime = entry.getValue().getStringByField("date");
            datetime += " " + entry.getValue().getStringByField("time");

            String mark = "";
            if (rbc.isSelected()) {
                List<Integer> temps = new ArrayList<>();
                if (k24Map.containsKey(key)) { temps.add(k24Map.get(key).getIntegerByField("temperature")); }
                temps.add(entry.getValue().getIntegerByField("temperature"));
                if (okairosMap.containsKey(key)) { temps.add(okairosMap.get(key).getIntegerByField("temperature")); }
                int delta = Collections.max(temps) - Collections.min(temps);
                if (rbAbsolute.isSelected()) {
                    if (delta >= spAbsolute.getValue()) {
                        mark = "*";
                    }
                } else if (rbPercentage.isSelected()) {
                    if (delta / (double) Collections.max(temps) >= spPercentage.getValue() / 100.0) {
                        mark = "*";
                    }
                }
            } else if (rbk.isSelected()) {
                List<Integer> knots = new ArrayList<>();
                if (k24Map.containsKey(key)) { knots.add(k24Map.get(key).getIntegerByField("knots")); }
                knots.add(entry.getValue().getIntegerByField("knots"));
                if (okairosMap.containsKey(key)) { knots.add(okairosMap.get(key).getIntegerByField("knots")); }
                int delta = Collections.max(knots) - Collections.min(knots);
                if (delta >= spAbsolute.getValue()) {
                    mark = "*";
                } else if (rbPercentage.isSelected()) {
                    if (delta / (double) Collections.max(knots) >= spPercentage.getValue() / 100.0) {
                        mark = "*";
                    }
                }
            } else if (rbh.isSelected()) {
                List<Integer> hums = new ArrayList<>();
                if (k24Map.containsKey(key)) { hums.add(k24Map.get(key).getIntegerByField("humidity")); }
                hums.add(entry.getValue().getIntegerByField("humidity"));
                if (okairosMap.containsKey(key)) { hums.add(okairosMap.get(key).getIntegerByField("humidity")); }
                int delta = Collections.max(hums) - Collections.min(hums);
                if (delta >= spAbsolute.getValue()) {
                    mark = "*";
                } else if (rbPercentage.isSelected()) {
                    if (delta / (double) Collections.max(hums) >= spPercentage.getValue() / 100.0) {
                        mark = "*";
                    }
                }
            }

            ArrayList<String> row = new ArrayList<>();
            row.add(datetime);
            row.add(mark);
            row.add(String.format("%-8s",
                (k24Map.containsKey(key) ? k24Map.get(key).getIntegerByField("temperature").toString() : "--") +
                (k24Delta.containsKey(key) ? String.format(" (%+02d)", k24Delta.get(key).getLeft()) : "")));
            row.add(String.format("%-8s",
                entry.getValue().getIntegerByField("temperature").toString() +
                (meteoDelta.containsKey(key) ? String.format(" (%+02d)", meteoDelta.get(key).getLeft()) : "")));
            row.add(String.format("%-8s",
                (okairosMap.containsKey(key) ? okairosMap.get(key).getIntegerByField("temperature").toString() : "--") +
                (okairosDelta.containsKey(key) ? String.format(" (%+02d)", okairosDelta.get(key).getLeft()) : "")));
            row.add(String.format("%-8s",
                (k24Map.containsKey(key) ? k24Map.get(key).getIntegerByField("knots").toString() : "--") +
                (k24Delta.containsKey(key) ? String.format(" (%+02d)", k24Delta.get(key).getMiddle()) : "")));
            row.add(String.format("%-8s",
                entry.getValue().getIntegerByField("knots").toString() +
                (meteoDelta.containsKey(key) ? String.format(" (%+02d)", meteoDelta.get(key).getMiddle()) : "")));
            row.add(String.format("%-8s",
                (okairosMap.containsKey(key) ? okairosMap.get(key).getIntegerByField("knots").toString() : "--")) +
                (okairosDelta.containsKey(key) ? String.format(" (%+02d)", okairosDelta.get(key).getMiddle()) : ""));
            row.add(String.format("%-8s",
                (k24Map.containsKey(key) ? k24Map.get(key).getIntegerByField("humidity").toString() : "--") +
                (k24Delta.containsKey(key) ? String.format(" (%+02d)", k24Delta.get(key).getRight()) : "")));
            row.add(String.format("%-8s",
                entry.getValue().getIntegerByField("humidity").toString() +
                (meteoDelta.containsKey(key) ? String.format(" (%+02d)", meteoDelta.get(key).getRight()) : "")));
            row.add(String.format("%-8s",
                (okairosMap.containsKey(key) ? okairosMap.get(key).getIntegerByField("humidity").toString(): "--") +
                (okairosDelta.containsKey(key) ? String.format(" (%+d)", okairosDelta.get(key).getRight()) : "")));

            table.getItems().add(row);
        }
    }
}