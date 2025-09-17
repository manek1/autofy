# -*- coding: utf-8 -*-
# PySpark + PyDeequ port of your Scala Deequ pipeline
# Author: ChatGPT for Azad

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pydeequ.verification import VerificationSuite, Check, CheckLevel
from pydeequ.verification import VerificationResult
# successMetricsAsDataFrame & checkResultsAsDataFrame are exposed off VerificationResult
# but in PyDeequ they are top-level helpers:
from pydeequ.repository import *
from pydeequ.checks import *
import datetime

def main():
    spark = (
        SparkSession.builder.appName("DQ Runner - CONTACT")
        # .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.3-spark-3.3")  # if needed inline
        .getOrCreate()
    )

    # -----------------------------
    # Config paths (same as Scala)
    # -----------------------------
    str_generalPath = "s3://intl-euro-uk-datascientist-prod/Data Quality Assessment/Use Cases/"
    str_useCaseRunning = "CONTACT"

    str_sourcesConfigPath = f"{str_generalPath}{str_useCaseRunning}/Config/sources.csv"
    str_fieldsAndRulesConfigPath = f"{str_generalPath}{str_useCaseRunning}/Config/fields_rules.csv"

    # ------------------------------------
    # Load configs (note: different delims)
    # ------------------------------------
    dt_sourceToUse = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .load(str_sourcesConfigPath)
    )

    dt_fieldsToUse = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ";")
        .load(str_fieldsAndRulesConfigPath)
    )

    # ------------------------------------
    # Extract single source parameters
    # ------------------------------------
    # (Assumes one active source row)
    str_extractionSQLStatement = dt_sourceToUse.select("sql_statement").first()[0]
    str_tableName = dt_sourceToUse.select("table_name").first()[0]
    str_databaseName = dt_sourceToUse.select("database_name").first()[0]
    str_whereClause = dt_sourceToUse.select("where_clause").first()[0]

    # ------------------------------------
    # Load source data via SQL
    # ------------------------------------
    df = spark.sql(str_extractionSQLStatement)

    # ====================================
    # 1) COMPLETENESS (isComplete)
    # ====================================
    # Scala did: dt_fields.drop("id", "checkUniqueness", "validityRule").dropDuplicates().as[ColumnName]
    dfCheckCompleteness = (
        dt_fieldsToUse
        .drop("id", "checkUniqueness", "validityRule")
        .dropDuplicates(["columnName"])
        .select("columnName")
        .where(F.col("columnName").isNotNull())
    )

    check_comp = Check(spark, CheckLevel.Error, "Claims Review Check")
    for r in dfCheckCompleteness.collect():
        colname = r["columnName"]
        check_comp = check_comp.isComplete(colname)

    verif_res_comp = VerificationSuite(spark).onData(df).addCheck(check_comp).run()
    checkResult_df = VerificationResult.successMetricsAsDataFrame(spark, verif_res_comp)

    # ====================================
    # 2) UNIQUENESS (isUnique where checkUniqueness == "1")
    # ====================================
    dfCheckUniqueness = (
        dt_fieldsToUse
        .where(F.col("checkUniqueness") == F.lit("1"))
        .drop("id", "checkUniqueness", "validityRule")
        .dropDuplicates(["columnName"])
        .select("columnName")
        .where(F.col("columnName").isNotNull())
    )

    check_unique = Check(spark, CheckLevel.Error, "Claims Review Check")
    for r in dfCheckUniqueness.collect():
        colname = r["columnName"]
        check_unique = check_unique.isUnique(colname)

    verif_res_unique = VerificationSuite(spark).onData(df).addCheck(check_unique).run()
    checkResult_df2 = VerificationResult.successMetricsAsDataFrame(spark, verif_res_unique)

    # ====================================
    # 3) VALIDITY (satisfies: validityRule on columnName)
    # ====================================
    validityRules = dt_fieldsToUse.select("columnName", "validityRuleDescription")

    dfCheckValidity = (
        dt_fieldsToUse
        .where((F.col("validityRule").isNotNull()) & (F.trim(F.col("validityRule")) != ""))
        .select("columnName", "validityRule")
    )

    check_valid = Check(spark, CheckLevel.Error, "Claims Review Check")
    for r in dfCheckValidity.collect():
        rule = r["validityRule"]
        colname = r["columnName"]
        # Deequ satisfies(rule, column, hint=None)
        check_valid = check_valid.satisfies(rule, colname)

    verif_res_valid = VerificationSuite(spark).onData(df).addCheck(check_valid).run()
    checkResult_df3_pre = VerificationResult.successMetricsAsDataFrame(spark, verif_res_valid)

    # Join back descriptions by matching instance (= columnName)
    checkResult_df3 = (
        checkResult_df3_pre.alias("m")
        .join(validityRules.alias("vr"), F.col("vr.columnName") == F.col("m.instance"), "inner")
        .drop("columnName")
    )

    # ====================================
    # 4) DISTINCTNESS (hasDistinctness >= 1)
    # ====================================
    dfCheckDistinctness = (
        dt_fieldsToUse
        .drop("id", "checkUniqueness", "validityRule")
        .dropDuplicates(["columnName"])
        .select("columnName")
        .where(F.col("columnName").isNotNull())
    )

    check_dist = Check(spark, CheckLevel.Error, "Claims Review Check")
    for r in dfCheckDistinctness.collect():
        colname = r["columnName"]
        # In Deequ: hasDistinctness(columns, assertion)
        check_dist = check_dist.hasDistinctness([colname], lambda v: v >= 1)

    verif_res_dist = VerificationSuite(spark).onData(df).addCheck(check_dist).run()
    checkResult_df4 = VerificationResult.successMetricsAsDataFrame(spark, verif_res_dist)

    # ====================================
    # INVALID RECORDS (full + top 50 per column)
    # Recreate UNION ALL queries that negate validityRule per column
    # ====================================
    dfIdentifyInvalidRecords = dfCheckValidity  # (columnName, validityRule)

    # Helper to safely backtick database.table
    full_table = f"`{str_databaseName}`.`{str_tableName}`"

    # Build FULL invalids query
    union_parts = [
        f"SELECT 'dummy' as table, 'dummy' as column, 'dummy' as value, 0 as count FROM {full_table} WHERE 0=1"
    ]
    for r in dfIdentifyInvalidRecords.collect():
        colname = r["columnName"]
        rule = r["validityRule"]
        part = (
            "UNION ALL SELECT "
            f"'{str_tableName}' AS table, "
            f"'{colname}' AS column, "
            f"{colname} AS value, "
            f"count(*) AS count "
            f"FROM {full_table} "
            f"WHERE ({str_whereClause}) AND NOT ({rule}) "
            f"GROUP BY {colname}"
        )
        union_parts.append(part)
    full_invalid_sql = "\n".join(union_parts)
    df_invalidRecords = spark.sql(full_invalid_sql)

    finalInvalidRecordList = (
        df_invalidRecords.alias("inv")
        .join(
            dt_fieldsToUse.alias("cfg"),
            F.col("inv.column") == F.col("cfg.columnName"),
            "inner",
        )
        .drop("column")
        .drop("id")
        .drop("checkUniqueness")
        .drop("validityRule")
    )

    # Build TOP invalids (limit 50 per column)
    union_parts_top = [
        f"SELECT 'dummy' as table, 'dummy' as column, 'dummy' as value, 0 as count FROM {full_table} WHERE 0=1"
    ]
    for r in dfIdentifyInvalidRecords.collect():
        colname = r["columnName"]
        rule = r["validityRule"]
        part = (
            "UNION ALL (SELECT "
            f"'{str_tableName}' AS table, "
            f"'{colname}' AS column, "
            f"{colname} AS value, "
            f"count(*) AS count "
            f"FROM {full_table} "
            f"WHERE ({str_whereClause}) AND NOT ({rule}) "
            f"GROUP BY {colname} "
            f"ORDER BY count DESC LIMIT 50)"
        )
        union_parts_top.append(part)
    top_invalid_sql = "\n".join(union_parts_top)
    df_invalidRecordsTop = spark.sql(top_invalid_sql)

    finalInvalidRecordListTop = (
        df_invalidRecordsTop.alias("inv")
        .join(
            dt_fieldsToUse.alias("cfg"),
            F.col("inv.column") == F.col("cfg.columnName"),
            "inner",
        )
        .drop("column")
        .drop("id")
        .drop("checkUniqueness")
        .drop("validityRule")
    )

    # ====================================
    # MERGE METRICS (same as Scala joins)
    # ====================================
    # result1..result4 with renamed columns
    result1 = (
        checkResult_df
        .drop("entity", "name")
        .withColumnRenamed("instance", "instanceC")
        .withColumnRenamed("value", "Completeness")
    )
    result2 = (
        checkResult_df2
        .drop("entity", "name")
        .withColumnRenamed("instance", "instanceU")
        .withColumnRenamed("value", "Uniqueness")
    )
    result3 = (
        checkResult_df3
        .drop("entity", "name")
        .withColumnRenamed("instance", "instanceV")
        .withColumnRenamed("value", "Validity")
    )
    result4 = (
        checkResult_df4
        .drop("entity", "name")
        .withColumnRenamed("instance", "instanceD")
        .withColumnRenamed("value", "Distinctness")
    )

    # join1: coalesce instanceC/instanceU -> instanceUC
    join1 = (
        result1.join(result2, result1["instanceC"] == result2["instanceU"], "outer")
        .withColumn(
            "instanceUC",
            F.when(F.col("instanceC").isNull(), F.col("instanceU")).otherwise(F.col("instanceC"))
        )
        .drop("instanceC", "instanceU")
    )

    # join2: bring in Distinctness
    join2 = (
        join1.join(result4, join1["instanceUC"] == result4["instanceD"], "outer")
        .withColumn(
            "instanceUD",
            F.when(F.col("instanceUC").isNull(), F.col("instanceD")).otherwise(F.col("instanceUC"))
        )
        .drop("instanceD", "instanceUC")
    )

    # final join: bring in Validity
    total_count = df.count()
    finalResult = (
        join2.join(result3, join2["instanceUD"] == result3["instanceV"], "outer")
        .withColumn(
            "colname",
            F.when(F.col("instanceUD").isNull(), F.col("instanceV")).otherwise(F.col("instanceUD"))
        )
        .withColumn("total_count", F.lit(total_count))
        .withColumn("unique", F.round(F.col("Completeness") * F.col("Uniqueness") * F.col("total_count"), 0).cast("int"))
        .withColumn("missing", F.round((F.lit(1) - F.col("Completeness")) * F.col("total_count"), 0).cast("int"))
        .withColumn("valid_count", F.round(F.col("Validity") * F.col("total_count"), 0).cast("int"))
        .withColumn("invalid", (F.col("total_count") - F.col("missing") - F.col("valid_count")).cast("int"))
        .withColumn("distinct", F.round(F.col("Completeness") * F.col("Distinctness") * F.col("total_count"), 0).cast("int"))
        .withColumn("missing_pct", (F.lit(1) - F.col("Completeness")))
        .withColumnRenamed("Uniqueness", "unique_pct")
        .withColumnRenamed("Completeness", "complete_pct")
        .withColumnRenamed("Validity", "valid_pct")
        .withColumnRenamed("Distinctness", "distinct_pct")
        .withColumn("table_name", F.lit(str_tableName))
        .withColumn("database_name", F.lit(str_databaseName))
        .drop("instanceUD", "instanceV")
        .withColumnRenamed("colname", "column")
    )

    # ------------------------------------
    # Output paths and write
    # ------------------------------------
    str_curDate = datetime.datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]  # YYYYMMdd_HHmmssSSS
    str_outputPath = f"{str_generalPath}{str_useCaseRunning}/Executions/{str_curDate}"

    # Repartition(1) for single CSV per folder (like Scala)
    (
        finalResult
        .repartition(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{str_outputPath}/summary")
    )
    (
        finalInvalidRecordList
        .repartition(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{str_outputPath}/invalid_records")
    )
    (
        finalInvalidRecordListTop
        .repartition(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{str_outputPath}/invalid_records_top")
    )

    print(f"✅ DQ outputs written under: {str_outputPath}")

    spark.stop()


if __name__ == "__main__":
    main()
