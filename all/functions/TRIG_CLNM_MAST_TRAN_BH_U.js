exports = async function (changeEvent) {
  const mongodb = context.services.get("mongodb-atlas");
  const db = mongodb.db("OPUS_SCRUB"); // update if different

  const clnmMast = db.collection("CLNM_MAST");
  const clmtMastTranBh = db.collection("CLMT_MAST_TRAN_BH");

  const oldDoc = changeEvent.fullDocumentBeforeChange; // requires pre-images
  const newDoc = changeEvent.fullDocument; // requires Full Document Lookup

  if (!oldDoc || !newDoc) {
    console.log(
      "Missing pre-image or fullDocument. Enable pre-images and Full Document Lookup.",
    );
    return;
  }

  try {
    // Get DUMMY_IND from parent using OLD.CLM_NO
    const parent = await clnmMast.findOne({ CLM_NO: oldDoc.CLM_NO });
    if (parent?.DUMMY_IND !== "Y") return;

    const count = await clmtMastTranBh.count({
      UKEY_BH: oldDoc.UKEY_BH,
    });
    if (count == 0) return;

    const res = await clmtMastTranBh.updateMany(
      { UKEY_BH: oldDoc.UKEY_BH },
      {
        $set: {
          CLM_NO: newDoc.CLM_NO,
          ITEM_NO: newDoc.ITEM_NO,
          TRAN_NO: newDoc.TRAN_NO,
        },
      },
    );

    console.log(`Updated CLMT_MAST_TRAN_BH for UKEY_BH=${oldDoc.UKEY_BH}`);
  } catch (e) {
    console.error("Trigger error:", e.message);
  }
};
