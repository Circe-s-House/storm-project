package storm_project;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.InputDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.MapChangeListener;
import javafx.collections.ObservableMap;
import javafx.concurrent.Task;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ListView;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

public class App extends Application {
    public static TextArea dataArea;
    public static ObservableMap<Long, Tuple> k24Map;
    public static ObservableMap<Long, Tuple> meteoMap;
    public static ObservableMap<Long, Tuple> okairosMap;
    private static TableView<String> table;
    public static void main(String[] args) throws FileNotFoundException, IOException, TException, Exception {
        TopologyBuilder builder = new TopologyBuilder();
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
        makeThread("k24", 3).start();
        makeThread("meteo", 3).start();
        makeThread("okairos", 1).start();

        BorderPane mainPane = new BorderPane();

        VBox mainVPane = new VBox();
        dataArea = new TextArea();
        dataArea.setMaxWidth(300);
        dataArea.setMaxHeight(1200);
        Button but1 = new Button("meteo");
        but1.setPrefWidth(100);
        but1.setPrefHeight(20);
        Button but2 = new Button("okairos");
        but2.setPrefWidth(100);
        but2.setPrefHeight(20);
        Button but3 = new Button("k24");
        but3.setPrefWidth(100);
        but3.setPrefHeight(20);
        HBox buttonPane = new HBox();
        buttonPane.getChildren().addAll(but1, but2, but3);
        TextArea msgArea = new TextArea();
        msgArea.setMaxWidth(300);
        mainVPane.getChildren().addAll(dataArea, buttonPane,msgArea);

        mainPane.setLeft(mainVPane);

        table = new TableView<>();
        table.getColumns().add(new TableColumn<String, String>("time"));
        table.getColumns().add(new TableColumn<String, String>("k24"));
        table.getColumns().add(new TableColumn<String, String>("meteo"));
        table.getColumns().add(new TableColumn<String, String>("okairos"));
        mainPane.setCenter(table);

        Scene scene = new Scene(mainPane, 1200, 500);
        but1.setOnAction((event) -> { runSpider("k24"); });
        but2.setOnAction((event) -> { runSpider("meteo"); });
        but3.setOnAction((event) -> { runSpider("okairos"); });

        stage.setScene(scene);
        stage.show();

        k24Map = makeMap(52);
        meteoMap = makeMap(44);
        okairosMap = makeMap(252 / 3);
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
        } catch (IOException e) {}
    }

    public static void runSpider(String site) {
        runCmd("dd if=/dev/null of=data/" + site + ".csv");
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
                    for (Map.Entry<Long, Tuple> entry : k24Map.entrySet()) {
                        long key = entry.getKey();
                        String datetime = entry.getValue().getStringByField("date");
                        datetime += " " + entry.getValue().getStringByField("time");

                        //table.getItems().add(datetime, "-", "-", "-");
                        // table.getItems().add(entry.getValue().getIntegerByField("temperature").toString());
                        // if (meteoMap.containsKey(key)) {
                        //     table.getItems().add(meteoMap.get(key).getIntegerByField("temperature").toString());
                        // } else {
                        //     table.getItems().add("-");
                        // }
                        // if (okairosMap.containsKey(key)) {
                        //     table.getItems().add(okairosMap.get(key).getIntegerByField("temperature").toString());
                        // } else {
                        //     table.getItems().add("-");
                        // }
                    }
                }
            }
        });
        return observableMap;
    }
}