exports = async function (changeEvent) {
  const db = context.services.get("mongodb-atlas").db("OPUS_SCRUB"); // adjust DB if needed
  const clnmMast = db.collection("CLNM_MAST");
  const clmtMastTranAh = db.collection("CLMT_MAST_TRAN_AH");

  const oldDoc = changeEvent.fullDocumentBeforeChange; // requires pre-images
  if (!oldDoc) {
    console.log("Pre-image not available; enable pre-images.");
    return;
  }

  try {
    const parent = await clnmMast.findOne({ CLM_NO: oldDoc.CLM_NO });
    if (parent?.DUMMY_IND !== "Y") return;

    const count = await clmtMastTranAh.count({ UKEY_AH: oldDoc.UKEY_AH });

    if (count !== 0) {
      const res = await clmtMastTranAh.deleteMany({
        UKEY_AH: oldDoc.UKEY_AH,
        TRAN_NO: oldDoc.TRAN_NO,
      });
    }
    console.log(
      `Deleted CLMT_MAST_TRAN_AH UKEY_AH=${oldDoc.UKEY_AH} TRAN_NO=${oldDoc.TRAN_NO}`,
    );
  } catch (e) {
    console.error("Trigger error:", e.message);
  }
};
