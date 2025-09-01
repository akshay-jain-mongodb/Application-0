exports = async function (changeEvent) {  
  const mongodb = context.services.get("mongodb-atlas");  
  const db = mongodb.db("OPUS_SCRUB"); // adjust if different  
  
  const clnmMast = db.collection("CLNM_MAST");  
  const clmtPymt = db.collection("CLMT_PYMT");  
  
  const oldDoc = changeEvent.fullDocumentBeforeChange;   // requires pre-images  
  const newDoc = changeEvent.fullDocument;               // requires Full Document Lookup  
  if (!oldDoc || !newDoc) {  
    console.log("Missing OLD/NEW docs. Enable pre-images and Full Document Lookup.");  
    return;  
  }  
  
  try {  
    // Gate by DUMMY_IND from CLNM_MAST using OLD.CLM_NO  
    const mast = await clnmMast.findOne({ CLM_NO: oldDoc.CLM_NO });  
    if ((mast?.DUMMY_IND || "N") !== "Y") return;  

    const count = await clmtPymt.count({
      PYMT_NO: oldDoc.PYMT_NO,
    });
    if (count == 0) return;
  
    // Update CLMT_PYMT only if it exists for OLD.PYMT_NO  
    const res = await clmtPymt.updateMany(  
      { PYMT_NO: oldDoc.PYMT_NO },  
      {  
        $set: {  
          PYMT_NO: newDoc.PYMT_NO,  
          TRAN_DATE: newDoc.TRAN_DATE,  
          TRAN_TIME: newDoc.TRAN_TIME,  
          EDATE: newDoc.EDATE,  
          CLM_NO: newDoc.CLM_NO,  
          POL_NO: newDoc.POL_NO,  
          OPERATOR: newDoc.OPERATOR,  
          ISSUE_OFFICE: newDoc.ISSUE_OFFICE,  
  
          OD_AMT: newDoc.RESV_AMT,           // map RESV_AMT -> OD_AMT  
          SAL_AMT: newDoc.SAL_AMT,  
          SUBR_AMT: newDoc.SUBR_AMT,  
          ADJ_AMT: newDoc.ADJ_AMT,  
          SOL_AMT: newDoc.SOL_AMT,  
  
          AGENT_ID: newDoc.AGENT_ID,  
          AGENT_CODE: newDoc.AGENT_CODE,  
          AGENT_CAT_TYPE: newDoc.AGENT_CAT_TYPE,  
          PYMT_TYPE: newDoc.PYMT_TYPE,  
          STATUS: newDoc.STATUS,  
          PYMT_AMT: newDoc.PYMT_AMT,  
  
          CONTRACT_ID: newDoc.CONTRACT_ID,  
          POLICY_VERSION: newDoc.POLICY_VERSION,  
          COV_CODE: newDoc.COV_CODE,  
          COV_ID: newDoc.COV_ID,  
          // PRODUCT_CODE/COV_MAINCLS intentionally not updated (per Oracle trigger)  
  
          LOB: newDoc.LOB,  
          CP_PART_ID: newDoc.CP_PART_ID,  
          CP_VERSION: newDoc.CP_VERSION,  
          EXT_PART_ID: newDoc.EXT_PART_ID,  
          EXT_PART_VERSION: newDoc.EXT_PART_VERSION,  
          BEN_PART_ID: newDoc.BEN_PART_ID,  
          BEN_PART_VERSION: newDoc.BEN_PART_VERSION,  
  
          PAYEE: newDoc.PAYEE,  
          PYMT_IND: newDoc.PYMT_IND,  
  
          DIFF_OD_AMT: newDoc.DIFF_RESV_AMT, // map DIFF_RESV_AMT -> DIFF_OD_AMT  
          DIFF_ADJ_AMT: newDoc.DIFF_ADJ_AMT,  
          DIFF_SOL_AMT: newDoc.DIFF_SOL_AMT,  
          DIFF_XOL_AMT: newDoc.DIFF_XOL_AMT,  
          DIFF_RI_AMT: newDoc.DIFF_RI_AMT,  
          BLOCK_ITC_IND: newDoc.BLOCK_ITC_IND,  
  
          INS_COV_PURPOSE: newDoc.INS_COV_PURPOSE,  
          SUB_PYMT_TYPE: newDoc.SUB_PYMT_TYPE,  
          PAYEE_GST_REG_IND: newDoc.PAYEE_GST_REG_IND,  
          PAYEE_GST_REG_DATE: newDoc.PAYEE_GST_REG_DATE,  
          PAYEE_GST_REG_NO: newDoc.PAYEE_GST_REG_NO,  
          PAYEE_SERVICE_COUNTRY: newDoc.PAYEE_SERVICE_COUNTRY,  
  
          TOT_BILL_CASH_AMT: newDoc.TOT_BILL_CASH_AMT,  
          TOT_GST_AMT: newDoc.TOT_GST_AMT,  
          TOT_PAYABLE: newDoc.TOT_PAYABLE,  
          TOT_ITC_CLAIMABLE: newDoc.TOT_ITC_CLAIMABLE,  
          TOT_OWN_EXPENSE: newDoc.TOT_OWN_EXPENSE,  
  
          GST_OTAX_INV_REFNO: newDoc.GST_OTAX_INV_REFNO,  
          GST_OTAX_INV_DATE: newDoc.GST_OTAX_INV_DATE,  
          GST_OTAX_INV_PRN_IND: newDoc.GST_OTAX_INV_PRN_IND  
        }  
      }  
    );  
  
      console.log(`Updated CLMT_PYMT for PYMT_NO(old)=${oldDoc.PYMT_NO} -> PYMT_NO(new)=${newDoc.PYMT_NO}`);  
  } catch (e) {  
    console.error("Trigger error:", e.message);  
  }  
};  
