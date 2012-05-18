
Examples
========

Add data to the store:

record.json

    {
      "color": "red",
      "year": "1995",
      "model": "mustang",
      "manufacturer": "ford",
      "mileage": 75000,
      "field1": val1,
      "field2": val1,
      "field3": val1
    }

    curl -d@record.json http://localhost:8080/records

Query data from the store:

    curl -d'sql=select * from data_index' http://localhost:8080/records/queries

    {"elements":[{"model":"mustang","field3":"val1","mileage":75000,"field2":"val1","color":"red","manufacturer":"ford","year":"1995","field1":"val1"}]}


Internals
=========

    {
      "color": "red",
      "year": "1995",
      "model": "mustang",
      "manufacturer": "ford",
      "mileage": 75000,
      "field1": val1,
      "field2": val1,
      "field3": val1
    }

    ==>

    {
      "color": h0,
      "year": h1,
      "model": h2,
      "manufacturer": h3,
      "mileage": h4,
      "field1": val1,
      "field2": val1,
      "field3": val1
    }


Schema Config
=============

    {
      "dimensions": 12,
      "fields": {
        "color": 3,
        "year": 3,
        "model": 2,
        "manufacturer": 2,
        "mileage": 2
      }
    }


Possibly fancier config:

    {
      "fields": {
        "color": {
          "alg": "md5",
          "weight": 3
        },
        "year": {
          "alg": "md5",
          "weight": 3
        },
        "model": {
          "alg": "md5",
          "weight": 2
        },
        "manufacturer": {
          "alg": "md5",
          "weight": 2
        },
        "mileage": {
          "alg": "md5",
          "weight": 2
        }
      }
    }

Todo
====

machine map
  localhost
  via zookeepr
