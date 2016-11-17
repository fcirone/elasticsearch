/*jslint node: true */
'use strict';

var es = require('elasticsearch');
var config = require('config');
var mongoClient = require('mongodb').MongoClient;

var esClient = es.Client({
  apiVersion: '2.3',
  requestTimeout: Infinity,
  hosts: config.eshost
});

esClient.count(function(err, response, status) {
  if (err) {
    console.log('err: ' + err);
  }
  
  console.log('Count: ' + response.count);
});

mongoClient.connect(config.mongourl, function(err, db) {
  if (err) {
    console.log('Err mongo connection: ' + err);
    return;
  }
  
  db.collection('statistics').aggregate(pipeline()).toArray(function(err, records) {
    if (err) {
      console.log('Err aggregate: ' + err);
      return;
    }
    
    var count = 0;
    
    records.forEach(function(record) {
      esClient.create({
        index: 'ticket-dev',
        type: 'ticket',
        body: record
      }, function(err, response) {
        if (err) {
          console.log('Err es.create: ' + err);
          return;
        }
        
        count++;
        
        console.log('document created on ES. Count: ' + count);
      });        
    });    
  });
});

function pipeline() {
  return [
    {
      $limit: 1000
    },
    {
      $project: {
        _id: 0,
        
        createDate : { 
          $concat: [
            { $substr: ["$createDate", 0, 4] }, '-',
            { $substr: ["$createDate", 5, 2] }, '-',
            { $substr: ["$createDate", 8, 2] }
          ]
        },
        type: "$type",
        data: "$data"
      }
    }
  ];
}  
