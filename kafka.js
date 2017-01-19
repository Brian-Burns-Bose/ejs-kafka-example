var eclairjs = require('eclairjs');

var spark = new eclairjs();

var KafkaUtils = require('eclairjs-kafka');
var kafkaUtils = new KafkaUtils({
  eclairjs: spark
});

spark.addModule(kafkaUtils);

var session = spark.sql.SparkSession.builder()
  .appName("Demo App")
  .getOrCreate();

var sparkContext = session.sparkContext();
var ssc = new spark.streaming.StreamingContext(
  sparkContext,
  new spark.streaming.Duration(250)
);

var kafkaHost = 'localhost:9092',
    topic = 'tlog';

var messages = kafkaUtils.createStream(
  ssc, "foo", kafkaHost, topic
).map(function(t) {
  return t._2();
});

messages.foreachRDD(
  function(rdd) {
    return rdd.collect();
  }, [],
  function(data) {
    console.log(data);
  }
).then(function() {
  console.log("starting streaming context");
  ssc.start();
}).catch(function(err) {
  console.log(err);
})

function exit() {
  process.exit(0);
}

function stop(e) {
  if (e) {
    console.log('Error:', e);
  }

  if (sparkContext) {
    sparkContext.stop().then(exit).catch(exit);
  }
}


process.on('SIGTERM', stop);
process.on('SIGINT', stop);
