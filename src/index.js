var kp = require('kafka-node');
var rp = require('request-promise');
var rand = require('randgen');

// Google api key to lookup addresses
var GKey = "";
var geocoder = require("node-geocoder")('google', 'https', { apiKey: GKey, formatter: 'json' });

// Walkscore Api key
var API_BASE = 'http://api.walkscore.com/score';
var API_KEY = '';

var kafka_zoopkeeper = "localhost:2181";

// Throttling to keep you in compliance with the api policies
var timeout = 3000;

// Where to center the search
var center_init = [0,0];
var center = center_init;


var batchSize = 5;
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
var client = new kp.Client(kafka_zoopkeeper);
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
  }, timeout);
});

producer.on('error', function(err) {
  console.log("Kafka Write Error");
  console.log(err);
  producer.close();
});



function processPoint() {
  var def = new Promise(function(res, rej) {

  })
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
    uri: API_BASE,
    qs: {
      format: 'json',
      address: address,
      lat: latitude,
      lon: longitude,
      wsapikey: API_KEY
    }
  })
}




function writeToQueue(data) {
  console.log("Adding a point");
  console.log(data.snappedLatitude + "::" + data.snappedLongitude + "\t\t" + data.address);
  kQueue.push(JSON.stringify(data));
  return data;
}


function send() {
  if(kQueue.length == batchSize) {
    console.log("Sending to Kafka");

    var batch = kQueue.splice(0, batchSize);
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