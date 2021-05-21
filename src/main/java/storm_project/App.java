package storm_project;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.InputDeclarer;
import org.apache.storm.topology.TopologyBuilder;

import javafx.application.Application;
import javafx.concurrent.Task;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TextArea;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

public class App extends Application {
    public static TextArea dataArea;
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

        HBox mainPane = new HBox();

        VBox mainVPane = new VBox();
        Button but1 = new Button("Scrape k24.gr");
        Button but2 = new Button("Scrape meteo.gr");
        Button but3 = new Button("Scrape okairos.net");
        dataArea = new TextArea();
        dataArea.setMaxWidth(300);
        dataArea.setMaxHeight(1200);
        mainVPane.getChildren().addAll(dataArea, but1, but2, but3);

        mainPane.getChildren().add(mainVPane);

        Scene scene = new Scene(mainPane, 1200, 500);
        but1.setOnAction((event) -> { runSpider("k24"); });
        but2.setOnAction((event) -> { runSpider("meteo"); });
        but3.setOnAction((event) -> { runSpider("okairos"); });

        stage.setScene(scene);
        stage.show();
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
}