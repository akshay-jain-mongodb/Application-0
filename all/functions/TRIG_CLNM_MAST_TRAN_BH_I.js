exports = async function (changeEvent) {
  const serviceName = "mongodb-atlas";
  const dbName = "OPUS_SCRUB";
  const db = context.services.get(serviceName).db(dbName);
  const clnmMast = db.collection("CLNM_MAST");
  const clmtMastTranBh = db.collection("CLMT_MAST_TRAN_BH");

  const newDoc = changeEvent.fullDocument;
  if (!newDoc) {
    console.log("No fullDocument on insert");
    return;
  }

  try {
    const parent = await clnmMast.findOne({ CLM_NO: newDoc.CLM_NO });
    const dummyInd = parent?.DUMMY_IND || "N";
    if (dummyInd !== "Y") return;

    const count = await clmtMastTranBh.count({ UKEY_BH: newDoc.UKEY_BH });
    if (count === 0) {
      await clmtMastTranBh.insertOne({
        UKEY_BH: newDoc.UKEY_BH,
        CLM_NO: newDoc.CLM_NO,
        ITEM_NO: newDoc.ITEM_NO,
        TRAN_NO: newDoc.TRAN_NO,
      });
      console.log(`Inserted CLMT_MAST_TRAN_BH for UKEY_BH=${newDoc.UKEY_BH}`);
    }
  } catch (err) {
    console.error("Trigger error:", err.message);
  }
};
