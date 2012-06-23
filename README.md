[Stored.io @ Github](https://github.com/briangu/stored.io)

Overview
========

Stored.io (store-d) lets you easily combine standard SQL servers to create a distributed schema-less JSON store that uses native SQL.

Putting developer convenience first, stored.io makes it easy to think of data in JSON and queries in standard SQL.  Rather than reinventing the wheel, stored.io leverages mature SQL systems to provide the backing stores.  Stored.io essentially is a smart, distributed computing wrapper.

Scaling is done by combining hyperspace hashing (see [hyperdex](http://hyperdex.org)) as "projections" and hypercube addressing (see [OLAP](http://en.wikipedia.org/wiki/Online_analytical_processing) and [Connection Machine](http://en.wikipedia.org/wiki/CM-2)) to map nodes and machines to regions of the hyperspace.  The layering provides a good deal of flexibility to manage skew.  Additionally, the hypercube design allows for straight foward replication strategies.

Projections are the foundation for both indexing and accessing data.  The fundamental function of a projection is to intelligently scope scatter-gather requests to an optimal node/machine subset.  Data may be indexed under multiple projections and accessed via multiple projections.  Projections may also be added and referenced dynamically.

From a developer's perspective, the system is schema-less because they never have to create them.  This is in the spirit of [MongoDB](http://www.mongodb.org/) and [Cassandra](http://cassandra.apache.org/).  The system also supports native SQL on the added JSON Objects.  When an object is added, the object is flattened automatically producing columns for each key-value path.

Initially, stored.io has a heavy bias towards read-oriented system of generally immutable data.  For example, indexing of Twitter tweets, news, rss, articles, stocks, etc.

stored.io is powered by [viper.io](https://github.com/briangu/viper.io).

A JDBC driver is under development.

Quick Start
===========

Get the code:

    $ git clone git://github.com/briangu/stored.io.git
    $ cd stored.io

Start server with defaults @ 8080:

    $ ./bin/launch.sh 8080

Load some test data:    

    $ ./bin/add_sample_data.sh

Use the python SQL repl: (Thanks Zoran!) [may require "easy_install restkit"]

    $ ./bin/repl

Try some queries:

    SQL: select color from cars
    SQL: select seat from cars
    SQL: select seat from cars where color = 'red' or color = 'blue'
    SQL: select seat.safety.rating from cars where color = 'red' or color = 'blue' and seat.material = 'leather'
    SQL: select seat.material from cars where seat.safety.rating = 8
    SQL: select user.id from tweets

Detailed Example
================

The core developer experience consists simply of posting JSON objects and querying them back out.  This section goes into details about what's actually happening to enable this simple developer experience.

Start a single node stored.io instance:

    $ cd stored.io
    $ ./bin/launch.sh 8080

OR build:

    $ ./bin/mvn-install.sh
    $ mvn clean install
    $ java -jar target/io.stored-0.0.1-SNAPSHOT-jar-with-dependencies.jar 8080 db/8080 src/main/resources/nodes.json src/main/resources/projections.json

When JSON is added, the system "flattens" the JSON creating path-specs for all fields and makes each a column in the underlying SQL db.  Subsequently, the developer can use any path-spec in a SQL statement.  If a column is also in the specified projection, upon creating the column it is also made an index.

To see this in action, create a file called record.json (@src/main/resources/record.json):

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

Add it to the store using the 'cars' projection:

    curl -d@src/main/resources/record.json -dprojection='cars' http://localhost:8080/records

Internally, for this JSON object the following columns are created in the SQL db:

    COLOR
    YEAR
    MODEL
    MANUFACTURER
    MILEAGE
    FIELD1
    FIELD2
    SEAT__MATERIAL
    SEAT__STYLE
    SEAT__SAFETY___RATING
    SEAT__SAFETY___BELTSTYLE

The cars projection uses the following columns as hyperspace dimensions so these columns will also be made indexes in the SQL db.

    COLOR
    YEAR
    MODEL
    MANUFACTURER

For convenience, the new SQL db columns can be referenced via SQL by:

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

Notice, no table schemas were explicitly defined!

Standard SQL is supported such that (cars is the hyperspace hashing projection we are using):

    curl --data-urlencode "sql=select seat.material from cars where seat.safety.rating = 5" http://localhost:8080/records/queries

Produces:

    {
      "seat": {
        "material": "leather"
      }
    }


Try some other queries:

    curl --data-urlencode "sql=select * from cars where seat.material = 'leather'" http://localhost:8080/records/queries

or

    curl --data-urlencode "sql=select * from cars where seat.safety.rating = 5" http://localhost:8080/records/queries

or SELECT a portion of the object:

    curl --data-urlencode "sql=select seat.material from cars where seat.safety.rating = 5" http://localhost:8080/records/queries

REST API
========

Store (INSERT)
--------------

##REQUEST:
    URL format:  http://<host>:<port>/records
    HTTP method: POST
    POST body keys:

        projection=<projection name>
        [record|records]=

##RESPONSE:
    id=[<record id>]

The specified projection name must refer to a previously registered projection.  Additionally, the record(s) being INSERTed must have at least one field which is present in the projection.  That is, the intersection between the record columns and the projection must be non-empty.

For inserting a single item, use the record key:

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

For doing a batch insert, use the records key:

    records=[{
      "color": "red",
      "year": "1995",
      "model": "mustang",
      "manufacturer": "ford",
      "mileage": 75000,
      "seat": {
        "material": "leather",
        "style": "bucket",
        "safety": {
          "rating": 5,
          "beltstyle": "5-point"
        }
      }
    },
    {
      "color": "blue",
      "year": "2006",
      "model": "prius",
      "manufacturer": "toyota",
      "mileage": 5000,
      "seat": {
        "material": "fabric",
        "style": "standard",
        "safety": {
          "rating": 8,
          "beltstyle": "shoulder"
        }
      }
    }]



Query (SELECT)
--------------

##REQUEST:
    URL format:  http://<host>:<port>/records/queries
    HTTP method: POST
    POST body:
        sql=<sql statement>

##RESPONSE:
    records=[<record>]

The specified SQL statement may be whichever SQL is supported by the underlying SQL engine that stored.io is configured to use.  However, there are a few constraints regarding the clause references:

NOTE: At this time, only the SELECT predicate is supported.

###SELECT

    Fields specified in the SELECT clause will extracted from the JSON records.
    COUNT will be supported.

###FROM

    The from clause MUST refer to a previously registered projection.

###WHERE

    Fields specified in the WHERE clause MUST refer to the fields indexed in previously INSERTed records.

###Examples:

    select * from cars
    select color from cars
    select seat from cars
    select seat from cars where color = 'red' or color = 'blue'
    select seat.safety.rating from cars where color = 'red' or color = 'blue' and seat.material = 'leather'
    select seat.material from cars where seat.safety.rating = 8
    select user.id from tweets

##TABLES and JOINS - NOT (YET?) SUPPORTED:

In order to get something, you have to give up something.  Since stored.io is schema-less, at this time, stored.io does not really have a notion of TABLE.  Undoubtedly, it's possible to wrangle some notion of table if desired.  For example, depending upon how stored.io is configured, it is possible for each projection to be stored in it's own table.  However, at this time, since tables are not supported, JOINS will not work.


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

Hyperspace Hashing
==================

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

Todo/Roadmap
============

Hadoop map/reduce-like functionality that can be distributed on each processing node.
Stream queries
Faceted search
Training mode, where the system recommends a projection based on query history.

Use zookeeper to configure cluster


