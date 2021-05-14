package storm_project;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.InputDeclarer;
import org.apache.storm.topology.TopologyBuilder;

import javafx.application.Application;
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
        builder.setSpout("meteoSpout", new CSVSpout("data/meteo.csv"));
        builder.setSpout("okairosSpout", new CSVSpout("data/okairos.csv"));
        builder.setSpout("k24Spout", new CSVSpout("data/k24.csv"));
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
        HBox mainPane = new HBox();

        VBox mainVPane = new VBox();
        Button but1 = new Button("Scrape meteo.gr");
        Button but2 = new Button("Scrape okairos.gr");
        Button but3 = new Button("Scrape k24.net");
        dataArea = new TextArea();
        dataArea.setMaxWidth(300);
        dataArea.setMaxHeight(1200);
        mainVPane.getChildren().addAll(dataArea, but1, but2, but3);

        mainPane.getChildren().add(mainVPane);

        Scene scene = new Scene(mainPane, 1200, 500);
        but1.setOnAction((event) -> {
            try {
                Runtime.getRuntime().exec("scrapy runspider spiders/meteo.py -t csv -o data/meteo.csv");
            } catch (IOException e) {}
        });
        but2.setOnAction((event) -> {
            try {
                Runtime.getRuntime().exec("scrapy runspider spiders/okairos.py -t csv -o data/okairos.csv");
            } catch (IOException e) {}
        });
        but3.setOnAction((event) -> {
            try {
                Runtime.getRuntime().exec("scrapy runspider spiders/k24.py -t csv -o data/k24.csv");
            } catch (IOException e) {}
        });

        stage.setScene(scene);
        stage.show();
    }

    @Override
    public void stop() { System.exit(0); }
}