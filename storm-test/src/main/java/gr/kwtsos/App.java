package gr.kwtsos;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.InputDeclarer;
import org.apache.storm.topology.TopologyBuilder;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.control.TextArea;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

public class App extends Application {
    public static TextArea dataArea = new TextArea();
    public static void main(String[] args) throws FileNotFoundException, IOException, TException, Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("CSVSpout", new CSVSpout("OpenData.csv"));
        InputDeclarer declarer = builder.setBolt("AverageBolt", new AverageBolt());
        declarer.shuffleGrouping("CSVSpout");

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
        VBox mainPane = new VBox();
        Scene scene = new Scene(mainPane, 500, 180);
        mainPane.getChildren().add(dataArea);
        stage.setScene(scene);
        stage.show();
    }

    @Override
    public void stop() { System.exit(0); }
}