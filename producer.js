const Kafka = require('kafka-node');
const config  = require('./config');

const Producer = Kafka.Producer;
console.log('server host: ', config.KafkaHost);
console.log('topic: ', config.KafkaTopic);
const client = new Kafka.KafkaClient({kafkaHost: 'DESKTOP-1VG6H7L:9092'});
const producer = new Producer(client,  {requireAcks: 0, partitionerType: 2});



const pushDataToKafka =(dataToPush) => {

  try {
  let payloadToKafkaTopic = [{topic: 'kafkanewtopic', messages: JSON.stringify(dataToPush) }];
  console.log(payloadToKafkaTopic);
  producer.on('ready', async function() {
    producer.send(payloadToKafkaTopic, (err, data) => {
          console.log('data: ', data);
  });

  producer.on('error', function(err) {
    //  handle error cases here
  })
  })
  }
catch(error) {
  console.log(error);
}

};


const jsonData = require('./app_json.js');

pushDataToKafka(jsonData);
