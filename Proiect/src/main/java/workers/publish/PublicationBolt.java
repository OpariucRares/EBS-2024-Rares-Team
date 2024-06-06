package workers.publish;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

public class PublicationBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector basicOutputCollector) {
        String publication = input.getStringByField("publication");
        System.out.println("Received publication: " + publication);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields to declare since this is a terminal bolt
    }

}
