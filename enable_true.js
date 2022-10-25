const kafka = require('kafka-node');
const config = require('./config');
const WebSocket = require('ws');

try {
  const Consumer = kafka.Consumer;
 //const client = new kafka.KafkaClient({idleConnection: 24 * 60 * 60 * 1000,  kafkaHost: 'DESKTOP-K49Q8NM:9092'});
 const client = new kafka.KafkaClient({idleConnection: 24 * 60 * 60 * 1000,  kafkaHost: 'localhost:9092'});

 let consumer = new Consumer(
    client,
    [{ topic: 'throttle_enabled', partition: 0 }],
    {
      autoCommit: true,
      fetchMaxWaitMs: 1000,
      fetchMaxBytes: 1024 * 1024,
      encoding: 'utf8',
      // fromOffset: false
    }
  );
  consumer.on('message', async function(message) {
    console.log(
      'kafka ',
      JSON.parse(message.value)
    );
    sendMessage(message.value)
  })
  consumer.on('error', function(error) {
    //  handle error 
    console.log('error', error);
  });
}
catch(error) {
  // catch error trace
  console.log(error);
}


const users = new Set();

function sendMessage (message) {
    users.forEach((user) => {
        user.ws.send(JSON.stringify(message));
    });
}

const server = new WebSocket.Server({
        port: 8082
    },
    () => {
        console.log('Server started on port 8082');
    }
);

server.on('connection', (ws) => {
    const userRef = {
        ws,
    };
    users.add(userRef);

    ws.on('close', (code, reason) => {
        users.delete(userRef);
        console.log(`Connection closed: ${code} ${reason}!`);
    });
});