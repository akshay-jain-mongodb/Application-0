exports = async function (changeEvent) {
  const db = context.services.get("mongodb-atlas").db("OPUS_SCRUB"); // adjust DB name
  const clnmMast = db.collection("CLNM_MAST");
  const clmtMast = db.collection("CLMT_MAST");
  const clmtPymt = db.collection("CLMT_PYMT");

  const newDoc = changeEvent.fullDocument;
  if (!newDoc) {
    console.log("No fullDocument on insert");
    return;
  }

  try {
    // 1) Gate by DUMMY_IND
    const mast = await clnmMast.findOne(
      { CLM_NO: newDoc.CLM_NO }
    );
    console.log(`mast= ${mast}`);

    if ((mast?.DUMMY_IND || "N") !== "Y") return;

    // 2) Product/Cover from CLMT_MAST
    const cov = await clmtMast.findOne(
      { CLM_NO: newDoc.CLM_NO }
    );
    const vPRODUCT_CODE = cov?.PRODUCT_CODE ?? null;
    const vCOV_MAINCLS = cov?.COV_MAINCLS ?? null;

    console.log(
      `vPRODUCT_CODE= ${vPRODUCT_CODE} and vCOV_MAINCLS= ${vCOV_MAINCLS}`,
    );

    // 3) Existence check
    const count = await clmtPymt.count({ PYMT_NO: newDoc.PYMT_NO });

    // 4) Insert-only (mirror Oracle INSERT)

    if (count === 0) {
      const doc = {
        PYMT_NO: newDoc.PYMT_NO,
        TRAN_DATE: newDoc.TRAN_DATE,
        TRAN_TIME: newDoc.TRAN_TIME,
        EDATE: newDoc.EDATE,
        CLM_NO: newDoc.CLM_NO,
        POL_NO: newDoc.POL_NO,
        OPERATOR: newDoc.OPERATOR,
        ISSUE_OFFICE: newDoc.ISSUE_OFFICE,
        OD_AMT: newDoc.RESV_AMT, // map
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
        PROC_YR: newDoc.PROC_YR,
        PROC_MTH: newDoc.PROC_MTH,
        CONTRACT_ID: newDoc.CONTRACT_ID,
        POLICY_VERSION: newDoc.POLICY_VERSION,
        COV_CODE: newDoc.COV_CODE,
        COV_ID: newDoc.COV_ID,
        COV_MAINCLS: vCOV_MAINCLS, // override
        PRODUCT_CODE: vPRODUCT_CODE, // override
        LOB: newDoc.LOB,
        CP_PART_ID: newDoc.CP_PART_ID,
        CP_VERSION: newDoc.CP_VERSION,
        EXT_PART_ID: newDoc.EXT_PART_ID,
        EXT_PART_VERSION: newDoc.EXT_PART_VERSION,
        BEN_PART_ID: newDoc.BEN_PART_ID,
        BEN_PART_VERSION: newDoc.BEN_PART_VERSION,
        PAYEE: newDoc.PAYEE,
        PYMT_IND: newDoc.PYMT_IND,
        DIFF_OD_AMT: newDoc.DIFF_RESV_AMT, // map
        DIFF_TPD_AMT: 0,
        DIFF_TPI_AMT: 0,
        DIFF_ADJ_AMT: newDoc.DIFF_ADJ_AMT,
        DIFF_SOL_AMT: newDoc.DIFF_SOL_AMT,
        DIFF_XOL_AMT: newDoc.DIFF_XOL_AMT,
        DIFF_RI_AMT: newDoc.DIFF_RI_AMT,
        BLOCK_ITC_IND: newDoc.BLOCK_ITC_IND,
        TPD_AMT: 0,
        TPI_AMT: 0,
        KFK_AMT: 0,
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
        GST_OTAX_INV_PRN_IND: newDoc.GST_OTAX_INV_PRN_IND,
        OPERATION_IGNORE_IND: "Y",
      };

      await clmtPymt.insertOne(doc);

      console.log(
        `Inserted CLMT_PYMT for PYMT_NO=${newDoc.PYMT_NO}`,
      );
    }
  } catch (err) {
    console.error("Trigger error:", err.message);
  }
};
