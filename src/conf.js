// Added a separate config file
module.exports = {
  google: {
    apiKey: ""
  },
  walkScore: {
    url: 'http://api.walkscore.com/score',
    apiKey: ""
  },
  kafka: {
    zookeeperHost: "localhost:2181"
  },
  geography: {
    center_init: [0,0]
  },
  performance: {
    // Throttling to keep you in compliance with the api policies
    throttle: 3000,
    batchSize: 5
  }
}