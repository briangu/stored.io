Summary
=======

Store-D is a schema-less JSON store that supports native SQL over dynamically added columns.
It is also a distributed SQL storage system that uses hyperspace hashing and hypercube addressing schemes.

From a developer's perspective, it is a schema-less JSON store that supports native SQL.

From an operational perspective, it is a distribute system with a heavy bias towards read-oriented system of generally immutable data.
For example, indexing of Twitter tweets, news information, stocks, etc.

The general design is inspired by several well-known and lesser-known systems:

    HyperDex.org
    Cassandra
    Mongo
    CM-2 (Connection Machine 2)

Some of the principles align very well with OLAP

Usage
=====

The basic developer experience consists simply of posting JSON objects into the system.
The system "flattens" the JSON creating a key-paths for all contents and makes each unique key path a column in the db.
Subsquently, the developer can use any key path in a SQL statement.


Examples
========

The following example shows how to add data and query it back out.  Notice, no table schemas were defined!

create a file called record.json

    {
      "color": "red",
      "year": "1995",
      "model": "mustang",
      "manufacturer": "ford",
      "mileage": 75000,
      "field1": val1,
      "field2": val1,
      "seat": {
        "material": "leather",
        "style": "bucket",
        "safety": {
          "rating": 5,
          "belt_style": "5-point"
        }
      }
    }

Add data to the store:

    curl -d@record.json http://localhost:8080/records

Query data from the store using plain-old-sql:

    curl --data-urlencode "sql=select * from data_index where seat_material = 'leather'" http://localhost:8080/records/queries

or

    curl --data-urlencode "sql=select * from data_index where seat_safety_rating = 5" http://localhost:8080/records/queries

Everytime you add new data to the system, columns will automatically be created for you.

Internals
=========

This section will contain detailed info on how the distributed nature of store-d works.

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
