package gr.kwtsos;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.InputDeclarer;
import org.apache.storm.topology.TopologyBuilder;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Button;
import javafx.scene.control.TextArea;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

public class App extends Application {
    public static TextArea dataArea;
    public static LineChart<String, Number> chart;
    public static XYChart.Series<String, Number> sr;
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
        HBox mainPane = new HBox();

        VBox mainVPane = new VBox();
        Button but1 = new Button("Toggle spout");
        dataArea = new TextArea();
        dataArea.setMaxWidth(100);
        dataArea.setMaxHeight(500);
        mainVPane.getChildren().add(dataArea);
        mainVPane.getChildren().add(but1);

        mainPane.getChildren().add(mainVPane);

        CategoryAxis xAxis = new CategoryAxis();
        xAxis.setLabel("Time (sec)");
        xAxis.setAnimated(false);
        NumberAxis yAxis = new NumberAxis();
        yAxis.setLabel("Knots");
        yAxis.setAnimated(false);

        chart = new LineChart<>(xAxis, yAxis);
        chart.setTitle("Average Knots: --");
        chart.setPrefWidth(1000);
        chart.setData(FXCollections.<XYChart.Series<String, Number>>observableArrayList());
        sr = new XYChart.Series<>();
        chart.getData().add(sr);

        mainPane.getChildren().add(chart);

        Scene scene = new Scene(mainPane, 1200, 500);
        but1.setOnAction((event) -> {
            CSVSpout.stop = !CSVSpout.stop;
        });

        stage.setScene(scene);
        stage.show();
    }

    @Override
    public void stop() { System.exit(0); }
}