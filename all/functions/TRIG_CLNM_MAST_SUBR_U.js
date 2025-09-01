exports = async function (changeEvent) {
  const serviceName = "mongodb-atlas"; //context.environment.values["CLUSTER_NAME"]; // Cluster from environment settings
  const dbName = "OPUS_SCRUB"; //context.environment.values["DATABASE_NAME"]; // Database name from environment settings

  // Require pre-images enabled to read OLD document
  const oldDoc = changeEvent.fullDocumentBeforeChange;
  if (!oldDoc) {
    console.log("Pre-images not available; enable at cluster + collection.");
    return;
  }

  const clnmMast = context.services
    .get(serviceName)
    .db(dbName)
    .collection("CLNM_MAST");
  const clmtMastSubr = context.services
    .get(serviceName)
    .db(dbName)
    .collection("CLMT_MAST_SUBR");

  try {
    // 1) Get DUMMY_IND from parent using OLD.CLM_NO
    const parentDoc = await clnmMast.findOne({ CLM_NO: oldDoc.CLM_NO });
    const dummyInd = parentDoc?.DUMMY_IND || "N"; // If not found, default to 'N'
    if (dummyInd !== "Y") return;

    // 2) Ensure a target row exists for OLD.UKEY
    const count = await clmtMastSubr.count({
      UKEY: oldDoc.UKEY,
    });
    if (count == 0) return;

    // 3) Update target with OLD values
    await clmtMastSubr.updateMany(
      { UKEY: oldDoc.UKEY },
      {
        $set: {
          SEQ_NO: oldDoc.SEQ_NO,
          ITEM_NO: oldDoc.ITEM_NO,
          SUBR_COMP_SOL: oldDoc.SUBR_COMP_SOL,
          SUBR_TP_NAME: oldDoc.SUBR_TP_NAME,
          SUBR_TP_SOL: oldDoc.SUBR_TP_SOL,
          SUBR_TP_INSURER: oldDoc.SUBR_TP_INSURER,
        },
      },
    );
  } catch (e) {
    console.error("Trigger error:", e.message);
  }
};
