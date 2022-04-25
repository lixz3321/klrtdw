CREATE external TABLE `hudi`.`O_LIS_LCPOL` ( `GRPCONTNO` STRING, `GRPPOLNO` STRING, `CONTNO` STRING, `POLNO` STRING, `PROPOSALNO` STRING, `PRTNO` STRING, `CONTTYPE` STRING, `POLTYPEFLAG` STRING, `MAINPOLNO` STRING, `MASTERPOLNO` STRING, `KINDCODE` STRING, `RISKCODE` STRING, `RISKVERSION` STRING, `MANAGECOM` STRING, `AGENTCOM` STRING, `AGENTTYPE` STRING, `AGENTCODE` STRING, `AGENTGROUP` STRING, `AGENTCODE1` STRING, `SALECHNL` STRING, `HANDLER` STRING, `INSUREDNO` STRING, `INSUREDNAME` STRING, `INSUREDSEX` STRING, `INSUREDBIRTHDAY` STRING, `INSUREDAPPAGE` DOUBLE, `INSUREDPEOPLES` DOUBLE, `OCCUPATIONTYPE` STRING, `APPNTNO` STRING, `APPNTNAME` STRING, `CVALIDATE` STRING, `SIGNCOM` STRING, `SIGNDATE` STRING, `SIGNTIME` STRING, `FIRSTPAYDATE` STRING, `PAYENDDATE` STRING, `PAYTODATE` STRING, `GETSTARTDATE` STRING, `ENDDATE` STRING, `ACCIENDDATE` STRING, `GETYEARFLAG` STRING, `GETYEAR` DOUBLE, `PAYENDYEARFLAG` STRING, `PAYENDYEAR` DOUBLE, `INSUYEARFLAG` STRING, `INSUYEAR` DOUBLE, `ACCIYEARFLAG` STRING, `ACCIYEAR` DOUBLE, `GETSTARTTYPE` STRING, `SPECIFYVALIDATE` STRING, `PAYMODE` STRING, `PAYLOCATION` STRING, `PAYINTV` DOUBLE, `PAYYEARS` DOUBLE, `YEARS` DOUBLE, `MANAGEFEERATE` DOUBLE, `FLOATRATE` DOUBLE, `PREMTOAMNT` STRING, `MULT` DOUBLE, `STANDPREM` DOUBLE, `PREM` DOUBLE, `SUMPREM` DOUBLE, `AMNT` DOUBLE, `RISKAMNT` DOUBLE, `LEAVINGMONEY` DOUBLE, `ENDORSETIMES` DOUBLE, `CLAIMTIMES` DOUBLE, `LIVETIMES` DOUBLE, `RENEWCOUNT` DOUBLE, `LASTGETDATE` STRING, `LASTLOANDATE` STRING, `LASTREGETDATE` STRING, `LASTEDORDATE` STRING, `LASTREVDATE` STRING, `RNEWFLAG` DOUBLE, `STOPFLAG` STRING, `EXPIRYFLAG` STRING, `AUTOPAYFLAG` STRING, `INTERESTDIFFLAG` STRING, `SUBFLAG` STRING, `BNFFLAG` STRING, `HEALTHCHECKFLAG` STRING, `IMPARTFLAG` STRING, `REINSUREFLAG` STRING, `AGENTPAYFLAG` STRING, `AGENTGETFLAG` STRING, `LIVEGETMODE` STRING, `DEADGETMODE` STRING, `BONUSGETMODE` STRING, `BONUSMAN` STRING, `DEADFLAG` STRING, `SMOKEFLAG` STRING, `REMARK` STRING, `APPROVEFLAG` STRING, `APPROVECODE` STRING, `APPROVEDATE` STRING, `APPROVETIME` STRING, `UWFLAG` STRING, `UWCODE` STRING, `UWDATE` STRING, `UWTIME` STRING, `POLAPPLYDATE` STRING, `APPFLAG` STRING, `POLSTATE` STRING, `STANDBYFLAG1` STRING, `STANDBYFLAG2` STRING, `STANDBYFLAG3` STRING, `OPERATOR` STRING, `MAKEDATE` STRING, `MAKETIME` STRING, `MODIFYDATE` STRING, `MODIFYTIME` STRING, `WAITPERIOD` DOUBLE, `PAYRULECODE` STRING, `ASCRIPTIONRULECODE` STRING, `SALECHNLDETAIL` STRING, `RISKSEQNO` STRING, `COPYS` DOUBLE, `COMFEERATE` DOUBLE, `BRANCHFEERATE` DOUBLE, `PROPOSALCONTNO` STRING, `CONTPLANCODE` STRING, `CESSAMNT` DOUBLE, `STATEFLAG` STRING, `SUPPLEMENTARYPREM` DOUBLE, `ACCTYPE` STRING, `ETL_DT` STRING, `ETL_TM` STRING, `ETL_FG` STRING) PARTITIONED BY (etl_part string);
CREATE EXTERNAL TABLE `hudi`.`O_LIS_LCCONT`(`MANAGECOM` STRING, `RECEIVEOPERATOR` STRING, `CRS_BUSSTYPE` STRING, `PAYINTV` STRING, `FAMILYTYPE` STRING, `DISPUTEDFLAG` STRING, `RECEIVEDATE` STRING, `APPNTNO` STRING, `LOSTTIMES` STRING, `INSUREDIDNO` STRING, `PREM` STRING, `XACCNAME` STRING, `RECEIVETIME` STRING, `MULT` STRING, `PROPOSALTYPE` STRING, `GRPAGENTCODE` STRING, `APPNTIDTYPE` STRING, `APPROVEDATE` STRING, `APPNTNAME` STRING, `CONTPREMFEENO` STRING, `INSUREDSEX` STRING, `POLAPPLYDATE` STRING, `PROPOSALCONTNO` STRING, `CUSTOMERRECEIPTNO` STRING, `CINVALIDATE` STRING, `XBANKACCNO` STRING, `OUTPAYFLAG` STRING, `CONTPRINTLOFLAG` STRING, `APPROVETIME` STRING, `UWOPERATOR` STRING, `HANDLER` STRING, `AGENTGROUP` STRING, `SIGNTIME` STRING, `APPNTSEX` STRING, `INTLFLAG` STRING, `CARDFLAG` STRING, `MODIFYDATE` STRING, `GETPOLTIME` STRING, `PAYTODATE` STRING, `APPROVEFLAG` STRING, `HANDLERPRINT` STRING, `AGENTTYPE` STRING, `REMARK` STRING, `PRTNO` STRING, `INSUREDIDTYPE` STRING, `DIF` STRING, `DUEFEEMSGFLAG` STRING, `AGENTCOM` STRING, `GRPAGENTIDNO` STRING, `APPFLAG` STRING, `CONSIGNNO` STRING, `OPERATOR` STRING, `APPNTBIRTHDAY` STRING, `INSUREDBIRTHDAY` STRING, `INSUREDNO` STRING, `CVALIDATE` STRING, `BANKACCNO` STRING, `SALECHNLDETAIL` STRING, `INPUTDATE` STRING, `HANDLERDATE` STRING, `FIRSTTRIALDATE` STRING, `PEOPLES` STRING, `MAKEDATE` STRING, `STATEFLAG` STRING, `UWTIME` STRING, `INPUTTIME` STRING, `FIRSTTRIALTIME` STRING, `GRPCONTNO` STRING, `PAYMETHOD` STRING, `XPAYMODE` STRING, `GETPOLMODE` STRING, `CONTNO` STRING, `UWDATE` STRING, `MAKETIME` STRING, `GETPOLDATE` STRING, `MODIFYTIME` STRING, `COPYS` STRING, `APPNTIDNO` STRING, `GRPAGENTCOM` STRING, `CONTTYPE` STRING, `TEMPFEENO` STRING, `PASSWORD` STRING, `SALECHNL` STRING, `ACCTYPE` STRING, `AGENTCODE1` STRING, `INSUREDNAME` STRING, `CRS_SALECHNL` STRING, `SUMPREM` STRING, `SIGNCOM` STRING, `STATE` STRING, `PAYMODE` STRING, `LANG` STRING, `SIGNDATE` STRING, `INPUTOPERATOR` STRING, `FIRSTTRIALOPERATOR` STRING, `APPROVECODE` STRING, `CUSTOMGETPOLDATE` STRING, `DEGREETYPE` STRING, `PAYLOCATION` STRING, `CURRENCY` STRING, `FIRSTPAYDATE` STRING, `XBANKCODE` STRING, `PRINTCOUNT` STRING, `AGENTCODE` STRING, `FAMILYID` STRING, `AMNT` STRING, `PAYERTYPE` STRING, `PREMSCOPE` STRING, `POLTYPE` STRING, `UWCONFIRMNO` STRING, `ACCNAME` STRING, `GRPAGENTNAME` STRING, `BANKCODE` STRING, `UWFLAG` STRING, `EXECUTECOM` STRING, `KAFKA_OFFSET` STRING, `KAFKA_PARTITION` STRING, `KAFKA_TOPIC` STRING, `KAFKA_TIMESTAMP` STRING) PARTITIONED BY ( `ETL_PART` INT) ROW FORMAT SERDE 'ORG.APACHE.HADOOP.HIVE.QL.IO.PARQUET.SERDE.PARQUETHIVESERDE' STORED AS INPUTFORMAT 'ORG.APACHE.HUDI.HADOOP.HOODIEPARQUETINPUTFORMAT' OUTPUTFORMAT 'ORG.APACHE.HADOOP.HIVE.QL.IO.PARQUET.MAPREDPARQUETOUTPUTFORMAT' LOCATION 'HDFS://KLBIGDATA/HUDI/ODS/O_LIS_LCCONT' TBLPROPERTIES ( 'BUCKETING_VERSION'='2', 'DISCOVER.PARTITIONS'='TRUE', 'TRANSIENT_LASTDDLTIME'='1649925100');
