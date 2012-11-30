var fs=require('fs');
var Express=require("express");
var App=Express();

App.configure(function() {
  App.use(Express.cookieParser());
  App.use(Express.bodyParser());
  App.use(Express.methodOverride());
  App.use(App.router);
});

var BB_Mongo = require('./Components/Database.js').DB;

var DB = new BB_Mongo(
 'localhost', 
 27017, 
 'business_listings'
);

var Geo_DB=new BB_Mongo(
 'localhost',
 27017,
 'Geo'
);

var Solr=require('solr-client');
var client=Solr.createClient(
 "127.0.0.1", 
 8983, 
 "establishments"
);
var profileclient=Solr.createClient(
 "127.0.0.1", 
 8983, 
 "profiles"
);
var QS=require('querystring');

var PlacesHandle = require('google-places');
var Places = new PlacesHandle("AIzaSyBe1VPkbwH1SNWXeg2nCL4TBeN8XK4RVEU");


var Types=[
          "art_gallery",
          "bakery",
          "beauty_salon",
          "bicycle_store",
          "book_store",
          "car_dealer",
          "car_rental",
          "car_repair",
          "car_wash",
          "clothing_store",
          "convenience_store",
          "department_store",
          "electrician",
          "electronics_store",
          "florist",
          "funeral_home",
          "furniture_store",
          "gas_station",
          "general_contractor",
          "grocery_or_supermarket",
          "gym",
          "hair_care",
          "hardware_store",
          "home_goods_store",
          "jewelry_store",
          "laundry",
          "liquor_store",
          "locksmith",
          "lodging",
          "meal_delivery",
          "meal_takeaway",
          "movie_rental",
          "moving_company",
          "painter",
          "pet_store",
          "pharmacy",
          "plumber",
          "roofing_contractor",
          "shoe_store",
          "storage",
          "store",
          "travel_agency",
          "veterinary_care"
          ]

//AIzaSyAA3qUt--WTzbuWFtwweiK4yeQtVzPWBUA
//AIzaSyCz929H01utAp_RlCNe-23sQIA47pghQUw
//AIzaSyBe1VPkbwH1SNWXeg2nCL4TBeN8XK4RVEU
//AIzaSyAhkv2YKKGesHuMHd_DaUm4roqp1F9d3QA

function PlaceSearch(point, callback) {
 Places.search({
  location: point,
  types: Types,
  radius: 500
 },
 function(err,res) {
  if(err) {
   console.log(err);
  }
  else {
   callback(res);
  }
 });
}

function PlaceNextPageSearch(page_token, callback) {
 Places.search({
  pagetoken: page_token
 },
 function(err,res) {
  if(err) {
   console.log(err);
  }
  else {
   callback(res);
  }
 });
}

function PlaceDetailSearch(ref, callback) {
 Places.details({
  reference: ref
 },
 function(err, res) {
  if(err) {
   console.log(err);
  }
  else {
   console.log(res);
   callback(res);
  }
 });
}

function getZips(cb) {
 try {
  Geo_DB.query("zips", function(collection) {
   collection.find({"state_id":"NY"}).toArray(function(err, docs) {
    if(err) return console.log(err);
    for(var i=0;i<docs.length;i++) { cb(JSON.parse(docs[i].geojson)) }
   });
  });
 }
 catch(Error) {
  console.log(Error);
 }
}

function extractZipCoords(geoJSON, cb) {
 var boundary_path=null;
 if(geoJSON.features) {
  boundary_path=geoJSON.features[0].geometry.coordinates
 }
 else {
  boundary_path=geoJSON;
 }

 if(boundary_path.length > 1) {
  extractZipCoords(boundary_path);
 }
 else {
  cb(boundary_path);
 }
}

var Extract={
 Do: function() {
  getZips(function(coordinates) {
   extractZipCoords(coordinates, function(point) {
    console.log(point);
   });
  });
 }
};

var Search={
 Commit: function(docs, callback) {
  client.autoCommit=true;
  client.add(docs, function(err, obj) {
   if(err) return console.log(err);
   callback(obj);
  });
 },
 Query: {
  AroundMe:function(point, callback) {
   var query = client.createQuery().q('*:*');
   query.set(QS.stringify({
    fq: '{!geofilt}',
    sfield: 'coords',
    pt: point,
    d: 15,
    start: 0,
    rows: 10,
    sort: "geodist() asc"
   }));
   client.search(query, function(err,res) {
    if(err) {
     console.log(err);
     callback(err);
    }
    else {
     console.log(res);
     callback(res);
    }
   });
  },
  Profiles: function(cb) {
   var Query=profileclient.createQuery().q("*:*");
   Query.set(QS.stringify({
    start: 0,
    rows: 300
   }));
   profileclient.search(Query, function(err, response) {
    if(err) return console.log(err);
    cb(response.response);
   });
  }
 }
};

function composeEstablishmentObject(profile, cb) {
 var establishmentObject={
  formatted_address:profile.address,
  coords: profile.long + "," + profile.lat,
  lat: parseFloat(profile.lat),
  lng: parseFloat(profile.long),
  name: profile.name
 };
 cb(establishmentObject);
}

function composeEstablishmentObject(profile, cb) {
 var establishmentObject={
  formatted_address:profile.address,
  coords: profile.long + "," + profile.lat,
  lat: parseFloat(profile.lat),
  lng: parseFloat(profile.long),
  name: profile.name
 };
 cb(establishmentObject);
}

function migrateProfilesToEstablishmentsIndex() {
 Search.Query.Profiles(function(resultset) {
  for(var i=0;i<resultset.numFound;i++) {
   composeEstablishmentObject(resultset.docs[i], function(establishment) {
    Search.Commit(establishment, console.log);
   });
  }
 });
}

function iterCoords(path, callback) {
 if(path.length > 2) {
  for(var i=0;i<path.length;i++) {
   iterCoords(path[i]);
  }
 }
 else {
  callback(path);
 }
}

function commit_businesses() {
DB.query("business_details", function(collection) {
 collection.find().toArray(function(err, docs) {
  if(err) return console.log(err);
  docs.forEach(function(val, index) {
   var Details=val.result;
   
   Search.Commit({
    name: Details.name,
    formatted_address: Details.formatted_address,
    lat: Details.geometry.location.lat,
    lng: Details.geometry.location.lng,
    coords: Details.geometry.location.lat+","+Details.geometry.location.lng
   }, function(response) {
    console.log("Commiting "+Details.name);
    console.log(response);
    console.log("Done");
   });

  });
 });
});
}

function import_listings(coords) {
PlaceSearch(coords, function(response) {
 var nextPage=response.next_page_token;

 DB.query("business_listings", function(collection) {
  collection.insert(response.results, function(err, res) {
   if(err) return console.log(err);
   console.log(res);
   if(nextPage) {
   PlaceNextPageSearch(nextPage, function(response) {
    DB.query("business_listings", function(collection) {
     collection.insert(response.results, function(err, res) {
      if(err) return console.log(err);
      console.log(res);
      });
     });
    });
   }
  });
 });
});
}

function import_details_from_listings() {
DB.query("business_listings", function(collection) {
 collection.find().toArray(function(err, docs) {
  docs.forEach(function(val, key) {
   PlaceDetailSearch(val.reference, function(response) {
    DB.query("business_details", function(coll) {
     coll.insert(response, console.log);
    });
   });
  });
 });
});
}

App.get("/import", function(req,res) {
 if(req.query.location) {
  var Coords=req.query.location.split(",");
  import_listings(Coords);
  res.send("Listings Imported");
 }
});

App.get("/details", function(req, res) {
 import_details_from_listings();
 res.send("Details Imported");
});

App.get("/commit", function(req, res) {
 commit_businesses();
 res.send("Places Committed");
});

App.listen(8989);