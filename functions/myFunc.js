exports = async function (changeEvent) {
  console.log("Received changeEvent:" + JSON.stringify(changeEvent));

  var collection = context.services
    .get("myAtlasClusterEDU")
    .db("mygrocerylist")
    .collection("audit");

  try {
    // var result = await collection.insertOne({"storeLocation":changeEvent.fullDocument.storeLocation,
    //                                          "items":changeEvent.fullDocument.items});

    var result = await collection.insertOne({
      action: "insert",
      details: changeEvent.fullDocument,
      changeEvent: changeEvent,
    });
    return result;

    return result;
  } catch (err) {
    console.log("Failed to insert item: ", err.message);
    return { error: err.message };
  }
};

// In the Testing Console tab, paste the code below and click Run:
/*
exports({
  _id: {_data: '62548f79e7f11292792497cc' },
  operationType: 'insert',
  clusterTime: {
    "$timestamp": {
      t: 1649712420,
      i:6
    }
  },
  ns: {
    db: 'engineering',
    coll: 'users'
  },
  documentKey: {
    storeLocation: 'East Appleton',
    _id: "62548f79e7f11292792497cc"
  },
  fullDocument: {
    _id: "Sample User 1",
    value: 1002,
    _class: "com.learning.poc.beans.User"
  }
})
*/
