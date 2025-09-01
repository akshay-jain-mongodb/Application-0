exports = async function (changeEvent) {
  const serviceName = "mongodb-atlas"; // Replace with the verified Atlas service name
  const dbName = "OPUS_SCRUB"; // Replace with your database name
  const clnmMastColl = context.services
    .get(serviceName)
    .db(dbName)
    .collection("CLNM_MAST");
  const clmtMastSubrColl = context.services
    .get(serviceName)
    .db(dbName)
    .collection("CLMT_MAST_SUBR");
  
  // Retrieve the newly inserted document
  const newDoc = changeEvent.fullDocument;

  if (!newDoc) {
    console.error("No document found in the change event.");
    return;
  }

  try {
    // Step 1: Check 'DUMMY_IND' in the parent collection (CLNM_MAST)
    const clnmMastDoc = await clnmMastColl.findOne({ CLM_NO: newDoc.CLM_NO });
    const dummyInd = clnmMastDoc?.DUMMY_IND || "N"; // Default to 'N' if no matching document is found
    if (dummyInd === "Y") {
      // Step 2: Count documents in the child collection (CLMT_MAST_SUBR) matching 'UKEY'
      const count = await clmtMastSubrColl.count({
        UKEY: newDoc.UKEY,
      });

      // Step 3: If no matching rows exist, insert a new row
      if (count === 0) {
        const insertResult = await clmtMastSubrColl.insertOne({
          UKEY: newDoc.UKEY,
          CLM_NO: newDoc.CLM_NO,
          SEQ_NO: newDoc.SEQ_NO,
          ITEM_NO: newDoc.ITEM_NO,
          SUBR_COMP_SOL: newDoc.SUBR_COMP_SOL,
          SUBR_TP_NAME: newDoc.SUBR_TP_NAME,
          SUBR_TP_SOL: newDoc.SUBR_TP_SOL,
          SUBR_TP_INSURER: newDoc.SUBR_TP_INSURER,
        });

        console.log(
          `Inserted new document into CLMT_MAST_SUBR: ${JSON.stringify(insertResult)}`,
        );
      }
    }
  } catch (err) {
    console.error("Error processing trigger logic:", err.message);
  }
};
