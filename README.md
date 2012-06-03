Summary
=======

Stored.io (store-d) is a schema-less JSON object store that supports native SQL.
To make it scale, it is a distributed system that uses hyperspace hashing (projections) and hypercube addressing to map nodes to regions of the hyperspace.
Since projections are really useful, they may both be added dynamically and used dynamically (as the SQL from clause).

From a developer's perspective, the system is schemaless because they never have to create them.  This is in the spirit of MongoDB.
The system also supports native SQL on the added JSON Objects.
When an object is added, the object is flattened automatically producing columns for each key-value path.

From an operational perspective, it is a distribute system with a heavy bias towards read-oriented system of generally immutable data.
For example, indexing of Twitter tweets, news, rss, articles, stocks, etc.

The general design is inspired by several well-known and lesser-known systems:

    HyperDex.org
    Cassandra
    MongoDB
    CM-2 (Connection Machine 2)

Some of the principles align very well with OLAP

Quick Start
===========

Start server with defaults @ 8080:

    $ ./bin/launch.sh 8080

Load some test data:    

    $ ./bin/add_sample_data.sh

Use the SQL repl: (Thanks Zoran!)

    $ ./bin/repl

Try some queries:

    SQL: select color from cars
    SQL: select seat from cars
    SQL: select seat.material from cars where seat.safety.rating = 8
    SQL: select user.id from tweets

More Examples
=============

The basic developer experience consists simply of posting JSON objects into the system.
The system "flattens" the JSON creating key-paths for all contents and makes each unique key path a column in the db.
Subsequently, the developer can use any key-path in a SQL statement.

Object paths are selectable via the standard SQL select an yield a portion of the original JSON object.

For example, given:

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
          "beltstyle": "5-point"
        }
      }
    }

The following columns are created:

    color
    year
    model
    manufacturer
    mileage
    field1
    field2
    seat.material
    seat.style
    seat.safety.rating
    seat.safety.beltstyle

Standard SQL is supported such that (cars is the hyperspace hashing projection we are using):

    select seat.material from cars where seat.safety.rating = 5

Produces:

    {
      "seat": {
        "material": "leather"
      }
    }


Usage
========

Start the service by:

  java -jar target/io.stored-0.0.1-SNAPSHOT-jar-with-dependencies.jar src/main/resources/config.json src/main/resources/db

The following example shows how to add data and query it back out.  Notice, no table schemas were defined!

create a file called record.json

    record={
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
          "beltstyle": "5-point"
        }
      }
    }

Add data to the store:

    curl -d@record.json http://localhost:8080/records

Query data from the store using plain-old-sql:

    curl --data-urlencode "sql=select * from cars where seat.material = 'leather'" http://localhost:8080/records/queries

or

    curl --data-urlencode "sql=select * from cars where seat.safety.rating = 5" http://localhost:8080/records/queries

or SELECT a portion of the object:

    curl --data-urlencode "sql=select seat.material from cars where seat.safety.rating = 5" http://localhost:8080/records/queries

yielding:

    {
      "elements": [
        {
          "seat": {
              "material": "leather"
          }
        }
      ]
    }


Everytime you add new data to the system, columns will automatically be created for you.
However, only the columns specified in the hyperspace schema in config.json will be used to improve performance.

Projections
===========

    {
      "default": "cars",
      "projections" : [
        {
          "name": "cars",
          "dimensions": 12,
          "fields": {
            "color": 3,
            "year": 3,
            "model": 2,
            "manufacturer": 2,
            "mileage": 2
          }
        },
        {
          "name": "tweets",
          "doc": "TODO: support full-text search on text field",
          "dimensions": 10,
          "fields": {
            "user.id": 6,
            "user.screen_name": 2,
            "text": 2
          }
        }
      ]
    }

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
