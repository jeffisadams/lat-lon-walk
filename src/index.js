var kp = require('kafka-node');
var rp = require('request-promise');
var rand = require('randgen');

var conf = require('./conf');

console.log("=======================================");
console.log("             Start Config");
console.log("=======================================");

console.log(conf);

console.log("=======================================");
console.log("             End Config");
console.log("=======================================");

// Google api Init
var geocoder = require("node-geocoder")('google', 'https', { apiKey: conf.google.apiKey, formatter: 'json' });

// Where to center the search
var center_init = conf.geography.center;
var center = center_init;

// Holds an in memory simple queue of what to send.
// Currently there is no recourse if kafka send fails
// aside from a log, but something to think about
var kQueue = [];

// Stochastic properties
// These numbers were chosen without much thought
// And the iteration count is low enough that they aren't truly operating
// as a simulated annealing algorithm, but it was inspired by that thought process
var sigma_init = 10;
var sigma_min = 2;

var sigma = sigma_init;
var anneal = 10;

var scale = Math.pow(10, 3);

var threshold = 0.5;
var temp = 0.7;


// Init Kafka
var client = new kp.Client(conf.kafka.zookeeperHost);
var producer = new kp.HighLevelProducer(client);

// The Looping code
producer.on('ready', function() {
  var iter = 0;
  var lastScore = 0;

  setInterval(function(){
    processPoint()
      .then(function(walkObj) {
        send();
        // Cool the variance every (anneal) iterations
        if((iter % anneal == 0) && (iter > 0)) {
          sigma = sigma * temp;

          if(sigma < sigma_min) sigma = sigma_init;

          console.log("Annealing: " + sigma);
        }

        // If the walkscore is higher, go to there.
        if(walkObj.walkscore > lastScore) {
          center = [walkObj.snapped_lat, walkObj.snapped_lon];
        }
        // Randomly also move the center some of the time
        else if(rand.rnorm() > threshold) {
          center = [walkObj.snapped_lat, walkObj.snapped_lon];
        }

        iter++;
      });
  }, conf.performance.throttle);
});

producer.on('error', function(err) {
  console.log("Kafka Write Error");
  console.log(err);
  producer.close();
});




// Util Methods
function processPoint() {
  var point = getRandomPoint();
  var pointData = {};

  return getAddress(point)
    .then(function(res) {
      var parts = res[0].formattedAddress.split(",");
      parts.pop();

      pointData.address = parts.join(','); 
      pointData.latitude = res[0].latitude;
      pointData.longitude = res[0].longitude;
      pointData.city = res[0].city;
      pointData.zipcode = res[0].zipcode;
      pointData.county = res[0].administrativeLevels.level2long;
      pointData.state = res[0].administrativeLevels.level1short;
  
      return getWalkScore(pointData.address, pointData.latitude, pointData.longitude);
    })
    .then(function(walkScoreData) {
      var walkObj = JSON.parse(walkScoreData);

      pointData.walkScore = walkObj.walkscore;
      pointData.description = walkObj.description;
      pointData.snappedLatitude = walkObj.snapped_lat;
      pointData.snappedLongitude = walkObj.snapped_lon;
      
      writeToQueue(pointData);
      return walkObj;
    });
}

// Utils
function getRandomPoint() {
  var rand1 = rand.rnorm(0, sigma) / scale;
  var rand2 = rand.rnorm(0, sigma) / scale;
  return [center[0] + rand1, center[1] + rand2];
}

function getAddress(point) {
  return geocoder.reverse({ lat: point[0], lon: point[1] })
    .then(function(addressData){ return addressData; });
}

function getWalkScore(address, latitude, longitude) {
  return rp({
    uri: conf.walkScore.url,
    qs: {
      format: 'json',
      address: address,
      lat: latitude,
      lon: longitude,
      wsapikey: conf.walkScore.apiKey
    }
  })
}




function writeToQueue(data) {
  console.log("Adding a point");
  console.log(data.snappedLatitude + "::" + data.snappedLongitude + "\t\t" + data.address + "\t\t" + data.walkScore);
  kQueue.push(JSON.stringify(data));
  return data;
}


function send() {
  if(kQueue.length == conf.performance.batchSize) {
    console.log("Sending to Kafka");

    var batch = kQueue.splice(0, conf.performance.batchSize);
    producer.send(
      [{
        topic: 'walkscore',
        messages: batch
      }],
      function() {
        console.log("Send complete");
      }
    );
  }
}