exports = async function (changeEvent) {
  const db = context.services.get("mongodb-atlas").db("OPUS_SCRUB"); // adjust DB if needed
  const clnmMast = db.collection("CLNM_MAST");
  const clmtPymt = db.collection("CLMT_PYMT");

  const oldDoc = changeEvent.fullDocumentBeforeChange; // needs pre-images
  if (!oldDoc) {
    console.log("Pre-image not available; enable pre-images.");
    return;
  }

  try {
    const parent = await clnmMast.findOne({ CLM_NO: oldDoc.CLM_NO });

    const dummyInd = parent?.DUMMY_IND || "N";
    if (dummyInd !== "Y") return;

    const count = await clmtPymt.count({ PYMT_NO: oldDoc.PYMT_NO });
    if (count !== 0) {
      await clmtPymt.deleteMany({ PYMT_NO: oldDoc.PYMT_NO });
    }
    console.log(
      `Deleted CLMT_PYMT docs for PYMT_NO=${oldDoc.PYMT_NO}`,
    );
  } catch (e) {
    console.error("Trigger error:", e.message);
  }
};
