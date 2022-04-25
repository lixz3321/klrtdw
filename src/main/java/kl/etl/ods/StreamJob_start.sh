#!/usr/bin/env bash
# 注意要使用flink-1.13.6版本的
flink_version=1.13.6
/opt/flink-$flink_version/bin/flink run-application \
-t yarn-application \
-c kl.etl.ods.StreamJob \
-Dyarn.provided.lib.dirs="hdfs://klbigdata/user/dev/flink-$flink_version-dependency;hdfs://klbigdata/user/dev/flink-$flink_version-dependency/lib;hdfs://klbigdata/user/dev/flink-$flink_version-dependency/plugins" \
klrtdw-1.0.jar \
--topic listables-tmp \
--group etl_ods_group \
--broker ip-10-129-24-122.cn-northwest-1.compute.internal:9092 \
--ckp hdfs://klbigdata:8020/flink/ckp_klrtdw_ods \
--csv "tmp.csv" \
--interval 3000