package com.cloudwick.log;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import com.hmsonline.storm.cassandra.bolt.CassandraBatchingBolt;
import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.AckStrategy;
import backtype.storm.spout.SchemeAsMultiScheme;
import com.hmsonline.storm.cassandra.bolt.CassandraCounterBatchingBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.StringScheme;
import java.util.Arrays;
import java.util.HashMap;

/**
 * This class defines the storm topology, which links spouts and bolts
 */

public class LogTopology {

  public static void main(String[] args) throws Exception {

    Config config = new Config();
    config.setDebug(true);

    /*
     * Configure kafka spouts
     */
    //Conf.KAFKA_HOSTS.add(new Broker("localhost", 9092));
    SpoutConfig kafkaConf = new SpoutConfig(
            new ZkHosts("karthik-1.cloudwick.net:2181"),
            Conf.KAFKA_TOPIC,
            Conf.KAFKA_ZOOKEEPER_PATH,
            Conf.KAFKA_CONSUMER_ID);
    KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf);
    kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());

    /*
     * Configure cassandra bolt
     */
    HashMap<String,Object> clientConfig = new HashMap<String, Object>();
    clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, Conf.CASSANDRA_HOST);
    clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String[]{Conf.CASSANDRA_KEYSPACE}));
    config.put(Conf.CASSANDRA_CONFIG_KEY,clientConfig);
    // Create a bolt that writes to the "CASSANDRA_COUNT_CF_NAME" column family and uses the Tuple field
    // "LOG_TIMESTAMP" as the row key and "LOG_INCREMENT" as the increment value for atomic counter
    CassandraCounterBatchingBolt logPersistenceBolt = new CassandraCounterBatchingBolt(
        Conf.CASSANDRA_KEYSPACE,
        Conf.CASSANDRA_CONFIG_KEY,
        Conf.CASSANDRA_COUNT_CF_NAME,
        FieldNames.LOG_TIMESTAMP,
        FieldNames.LOG_INCREMENT);
    logPersistenceBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);
    //cassandra batching bolt to persist the status codes
    CassandraBatchingBolt<String,String,String> statusPersistenceBolt = new CassandraBatchingBolt<String, String, String>
            (Conf.CASSANDRA_CONFIG_KEY,
             new DefaultTupleMapper(Conf.CASSANDRA_KEYSPACE,Conf.CASSANDRA_STATUS_CF_NAME,FieldNames.LOG_STATUS_CODE)
            );
    statusPersistenceBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);
    // casssandra batching bolt to persist country counts
    CassandraBatchingBolt<String,String,String> countryStatsPersistenceBolt = new CassandraBatchingBolt<String, String, String>
            (Conf.CASSANDRA_CONFIG_KEY,
             new DefaultTupleMapper(Conf.CASSANDRA_KEYSPACE,Conf.CASSANDRA_COUNTRY_CF_NAME,FieldNames.COUNTRY)
            );
    countryStatsPersistenceBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);

    /*
     * Creates topology builder
     */
    TopologyBuilder builder = new TopologyBuilder();

    /*
     * Configure storm topology
     */
    builder.setSpout("spout", kafkaSpout, 2);
    builder.setBolt("parser", new ParseBolt(), 2).shuffleGrouping("spout");
    builder.setBolt("volumeCounterOneMin", new VolumeCountBolt(), 2).shuffleGrouping("parser");
    builder.setBolt("countPersistor", logPersistenceBolt, 2).shuffleGrouping("volumeCounterOneMin");
    builder.setBolt("ipStatusParser", new LogEventParserBolt(), 2).shuffleGrouping("parser");
    builder.setBolt("statusCounter",
        new StatusCountBolt(), 3).fieldsGrouping("ipStatusParser",
        new Fields(FieldNames.LOG_STATUS_CODE));
    builder.setBolt("statusCountPersistor", statusPersistenceBolt, 3).shuffleGrouping("statusCounter");
    builder.setBolt("geoLocationFinder", new GeoBolt(new IPResolver()), 3).shuffleGrouping("ipStatusParser");
    builder.setBolt("countryStats", new GeoStatsBolt(), 3).fieldsGrouping("geoLocationFinder",
        new Fields(FieldNames.COUNTRY));
    builder.setBolt("countryStatsPersistor", countryStatsPersistenceBolt, 3).shuffleGrouping("countryStats");
    builder.setBolt("printerBolt", new PrinterBolt(), 1).shuffleGrouping("countryStats");

    if(args!=null && args.length > 0) {
      // submit to cluster
      config.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], config, builder.createTopology());
    } else {
      // local cluster
      config.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("kafka", config, builder.createTopology());
      Thread.sleep(50000);
      cluster.shutdown();
    }
  }
}
