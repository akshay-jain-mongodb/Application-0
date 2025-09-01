exports = async function (changeEvent) {
  const serviceName = "mongodb-atlas"; //context.environment.values["CLUSTER_NAME"]; // Cluster from environment settings
  const dbName = "OPUS_SCRUB"; //context.environment.values["DATABASE_NAME"]; // Database name from environment settings
  const clnmMast = context.services
    .get(serviceName)
    .db(dbName)
    .collection("CLNM_MAST"); // Parent collection
  const clmtMastSubr = context.services
    .get(serviceName)
    .db(dbName)
    .collection("CLMT_MAST_SUBR"); // Child collection

  const deletedDoc = changeEvent.fullDocumentBeforeChange; // Contains _id/fields of deleted document

  try {
    // Verify the 'DUMMY_IND' field from the parent collection
    const parentDoc = await clnmMast.findOne({ CLM_NO: deletedDoc.CLM_NO });
    const dummyInd = parentDoc?.DUMMY_IND || "N"; // If not found, default to 'N'

    if (dummyInd === "Y") {
      // Count documents matching the UKEY in CLMT_MAST_SUBR
      const count = await clmtMastSubr.count({
        UKEY: deletedDoc.UKEY,
      });
      if (count > 0) {
        // Cascade delete matching documents
        await clmtMastSubr.deleteMany({ UKEY: deletedDoc.UKEY });
        console.log(`Deleted documents with UKEY: ${deletedDoc.UKEY}`);
      }
    }
  } catch (err) {
    console.error("Error processing trigger logic: ", err.message);
  }
};
