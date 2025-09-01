exports = async function (changeEvent) {
  const serviceName = "mongodb-atlas";
  const dbName = "OPUS_SCRUB";

  const db = context.services.get(serviceName).db(dbName);
  const clnmMast = db.collection("CLNM_MAST");
  const clmtMastTranBh = db.collection("CLMT_MAST_TRAN_BH");

  const deletedDoc = changeEvent.fullDocumentBeforeChange;
  if (!deletedDoc) {
    console.log(
      "No pre-image available; enable Change Stream Pre-Images at cluster and collection.",
    );
    return;
  }

  try {
    // Get DUMMY_IND from parent using OLD.CLM_NO
    const parentDoc = await clnmMast.findOne({ CLM_NO: deletedDoc.CLM_NO });
    const dummyInd = parentDoc?.DUMMY_IND || "N";
    if (dummyInd !== "Y") return;

    const count = await clmtMastTranBh.count({ UKEY_BH: deletedDoc.UKEY_BH });
    if (count !== 0) {
      await clmtMastTranBh.deleteMany({
        UKEY_BH: deletedDoc.UKEY_BH,
        TRAN_NO: deletedDoc.TRAN_NO,
      });
    }
  } catch (err) {
    console.error("Error processing trigger logic: ", err.message);
  }
};
