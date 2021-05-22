package storm_project;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.stage.Stage;

public class App extends Application {
    public static TextArea dataArea = new TextArea();
    public static ObservableMap<Long, Tuple> k24Map = makeMap(52);
    public static ObservableMap<Long, Tuple> meteoMap = makeMap(44);
    public static ObservableMap<Long, Tuple> okairosMap = makeMap(252 / 3);
    private static TableView<List<String>> table = new TableView<>();
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

        dataArea.setPrefWidth(420);
        Button but1 = new Button("k24");
        but1.setMinWidth(140);
        but1.setMinHeight(20);
        Button but2 = new Button("meteo");
        but2.setMinWidth(140);
        but2.setMinHeight(20);
        Button but3 = new Button("okairos");
        but3.setMinWidth(140);
        but3.setMinHeight(20);
        HBox buttonPane = new HBox();
        buttonPane.getChildren().addAll(but1, but2, but3);
        TextArea msgArea = new TextArea();
        msgArea.setPrefHeight(150);

        mainPane.setTop(buttonPane);
        mainPane.setBottom(msgArea);
        mainPane.setLeft(dataArea);

        table.setEditable(false);

        TableColumn<List<String>, String> timeCol = new TableColumn<>("time");
        timeCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().get(0)));
        timeCol.setMinWidth(215);
        table.getColumns().add(timeCol);
        TableColumn<List<String>, String> cCol = new TableColumn<>("°C");
        cCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().get(1)));
        cCol.setMinWidth(215);
        table.getColumns().add(cCol);
        TableColumn<List<String>, String> kCol = new TableColumn<>("knots");
        kCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().get(2)));
        kCol.setMinWidth(215);
        table.getColumns().add(kCol);
        TableColumn<List<String>, String> hCol = new TableColumn<>("humidity %");
        hCol.setCellValueFactory(data -> new SimpleStringProperty(data.getValue().get(3)));
        hCol.setMinWidth(215);
        table.getColumns().add(hCol);
        mainPane.setCenter(table);

        Scene scene = new Scene(mainPane, 1280, 720);
        scene.getStylesheets().add(getClass().getResource("style.css").toExternalForm());
        but1.setOnAction((event) -> {
            runSpider("k24");
            msgArea.appendText("Scraped https://gr.k24.net/ellada/peloponnisos/kairos-tripoli-66?i=1\n");
        });
        but2.setOnAction((event) -> {
            runSpider("meteo");
            msgArea.appendText("Scraped https://www.meteo.gr/cf.cfm?city_id=36\n");
        });
        but3.setOnAction((event) -> {
            runSpider("okairos");
            msgArea.appendText("Scraped https://www.okairos.gr/τρίπολη.html?v=ωριαία\n");
        });

        stage.setScene(scene);
        stage.show();

        makeThread("k24", 3).start();
        makeThread("meteo", 3).start();
        makeThread("okairos", 1).start();
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
            Process p = Runtime.getRuntime().exec(cmd);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void runSpider(String site) {
        runCmd("scrapy runspider spiders/" + site + ".py -o data/" + site + ".csv");
    }

    public static Thread makeThread(String site, int delay) {
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

    public static ObservableMap<Long, Tuple> makeMap(int size) {
        Map<Long, Tuple> map = Collections.synchronizedMap(
            new LinkedHashMap<Long, Tuple>() {
                @Override
                protected boolean removeEldestEntry(final Map.Entry<Long, Tuple> eldest) {
                    return size() > size;
                }
            }
        );
        ObservableMap<Long, Tuple> observableMap = FXCollections.observableMap(map);
        observableMap.addListener(new MapChangeListener<Long, Tuple>() {
            @Override
            public void onChanged(MapChangeListener.Change<? extends Long, ? extends Tuple> change) {
                if(change.wasAdded() || change.wasRemoved()) {
                    table.getItems().clear();
                    for (Map.Entry<Long, Tuple> entry : okairosMap.entrySet()) {
                        long key = entry.getKey();
                        String datetime = entry.getValue().getStringByField("date");
                        datetime += " " + entry.getValue().getStringByField("time");

                        ArrayList<String> row = new ArrayList<>();
                        row.add(datetime);

                        ArrayList<String> temperature = new ArrayList<>();
                        temperature.add(
                            String.format("%2s",
                                k24Map.containsKey(key) ?
                                k24Map.get(key).getIntegerByField("temperature").toString() :
                                "--"));
                        temperature.add(
                            String.format("%2s",
                                meteoMap.containsKey(key) ?
                                meteoMap.get(key).getIntegerByField("temperature").toString() :
                                "--"));
                        temperature.add(
                            String.format("%2s",    
                                entry.getValue().getIntegerByField("temperature").toString()));
                        row.add(String.join(" ", temperature));

                        ArrayList<String> knots = new ArrayList<>();
                        knots.add(
                            String.format("%2s",
                                k24Map.containsKey(key) ?
                                k24Map.get(key).getIntegerByField("knots").toString() :
                                "--"));
                        knots.add(
                            String.format("%2s",
                                meteoMap.containsKey(key) ?
                                meteoMap.get(key).getIntegerByField("knots").toString() :
                                "--"));
                        knots.add(
                            String.format("%2s",
                                entry.getValue().getIntegerByField("knots").toString()));
                        row.add(String.join(" ", knots));

                        ArrayList<String> humidity = new ArrayList<>();
                        humidity.add(
                            String.format("%2s",
                                k24Map.containsKey(key) ?
                                k24Map.get(key).getIntegerByField("humidity").toString():
                                "--"));
                        humidity.add(
                            String.format("%2s",
                                meteoMap.containsKey(key) ?
                                meteoMap.get(key).getIntegerByField("humidity").toString() :
                                "--"));
                        humidity.add(
                            String.format("%2s",
                                entry.getValue().getIntegerByField("humidity").toString()));
                        row.add(String.join(" ", humidity));

                        table.getItems().add(row);
                    }
                }
            }
        });
        return observableMap;
    }
}