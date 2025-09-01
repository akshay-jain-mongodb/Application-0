exports = async function (changeEvent) {
  const db = context.services.get("mongodb-atlas").db("OPUS_SCRUB"); // adjust DB if needed
  const clnmMast = db.collection("CLNM_MAST");
  const clmtMastTranAh = db.collection("CLMT_MAST_TRAN_AH");

  const newDoc = changeEvent.fullDocument;
  if (!newDoc) {
    console.log("No fullDocument on insert");
    return;
  }

  try {
    const parent = await clnmMast.findOne({ CLM_NO: newDoc.CLM_NO });
    const dummyInd = parent?.DUMMY_IND || "N";
    if (dummyInd !== "Y") return;

    const count = await clmtMastTranAh.count({ UKEY_AH: newDoc.UKEY_AH });
    if (count === 0) {
      await clmtMastTranAh.insertOne({
        UKEY_AH: newDoc.UKEY_AH,
        CLM_NO: newDoc.CLM_NO,
        ITEM_NO: newDoc.ITEM_NO,
        TRAN_NO: newDoc.TRAN_NO,
      });
    }
    console.log(`Inserted CLMT_MAST_TRAN_AH for UKEY_AH=${newDoc.UKEY_AH}`);
  } catch (e) {
    console.error("Trigger error:", e.message);
  }
};
