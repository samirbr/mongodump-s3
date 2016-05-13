var fs = require('fs'),
    _ = require('underscore'),
    spawn = require('child_process').spawn,
    Deferred = require("promised-io/promise").Deferred,
    AWS = require('aws-sdk');
    mongodb = require('./mongodb');

var steps = [function mongodump() {

  var deferred = new Deferred();

  var config = this;
  var mongodumpArgs = {
      'host': 'localhost:27017',
      'db': '',
      'out': '/tmp',
      'query': '',
      'collection': '',
      'excludeCollectionsWithPrefix': '',
      'username': '',
      'password': ''
  };

  // Determine the command line arguments for the dump
  var args = Object.keys(mongodumpArgs).reduce(function(output, key) {
      if (key in config.mongo) {
        output.push('--' + key, config.mongo[key]);
      } else if (mongodumpArgs[key]) {
        output.push('--' + key, mongodumpArgs[key]);
      }
      
      return output;
  }, []);

  var mongodump = spawn('mongodump', args);
  mongodump.on('exit', function (code) {
    config.dumpDirectory = config.mongo.out + '/' + config.mongo.db;
    return deferred.resolve();
  });

  return deferred.promise;

}, function zip() {

  var deferred = new Deferred();

  var config = this;
  var timestamp = new Date().toISOString().replace(/\..+/g, '').replace(/[-:]/g, '').replace(/T/g, '-');
  var basename = config.basename ? config.basename + '.tar.gz' : config.mongo.db + '-' + timestamp + '.tar.gz';
  var filename = config.mongo.out + '/' + basename;

  var args = ['-zcvf', filename, '-C', config.dumpDirectory, '.'];
  var tar = spawn('tar', args);
  tar.on('exit', function (code) {
    config.basename = basename;
    config.filename = filename;
    deferred.resolve();
  });

  return deferred.promise;

}, function cleanDump() {

  var deferred = new Deferred();

  var args = ['-r', this.dumpDirectory];
  var rm = spawn('rm', args);
  rm.on('exit', function (code) {
    deferred.resolve();
  });

  return deferred.promise;

}, function uploadToAws() {

  var config = this;
  var deferred = new Deferred();

  var bucket = new AWS.S3({
      region: config.aws.region,
      accessKeyId: config.aws.accessKeyId,
      secretAccessKey: config.aws.secretAccessKey,
      params: {
          Bucket: config.aws.bucket
      }
  })
  bucket.putObject({
    Key: config.basename,
    Body: fs.createReadStream(config.filename)
  }, function(err, data) {
    if (err) return deferred.reject(err);
    console.log(data);
    config.href = data;
    deferred.resolve();
  });

  return deferred.promise;

}, function cleanZip() {

  var deferred = new Deferred();

  var args = [this.filename];
  var rm = spawn('rm', args);
  rm.on('exit', function (code) {
    deferred.resolve();
  });

  return deferred.promise;
}]

var Dump = function(config) {
  this.config = config;
}

_.extend(Dump.prototype, {
  exec: function() {
    var config = this.config;

    var boundSteps = _.map(steps, function(step) {
      return _.bind(step, config);
    });

    return require("promised-io/promise").seq(boundSteps).then(function() {
      return config;
    });
  }
});

module.exports = {
  Dump: Dump
}
