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

choices for auto-field mapping:
  sorting (can't deal well with new inserted fields)
  first come first serve, downside is each node may have a different mapping
  fixed mapping at the config level
goal: consistent mapping across nodes, automatically
  agreement via zk
  in localmode, singleton

how do we deal with value types?
dynamically add columns? alter scripts.

extract field path as column name
add column if not present: http://sqlite.org/lang_altertable.html


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


field map
  field hash functions
  field names -> bitwidth

dynamic field association to fixed sql fields in db


dimensions


machine map
  localhost
  via zookeepr
  machine mapping?
