# 注意要使用flink-1.13.6版本的
flink_version=1.13.6
/opt/flink-$flink_version/bin/flink run-application \
-t yarn-application \
-c kl.etl.ods.BatchJob  \
-Djobmanager.memory.process.size=2048mb \
-Dtaskmanager.memory.process.size=4096mb \
-Dtaskmanager.numberOfTaskSlots=2 \
-Dyarn.provided.lib.dirs="hdfs://klbigdata/user/dev/flink-$flink_version-dependency;hdfs://klbigdata/user/dev/flink-$flink_version-dependency/lib;hdfs://klbigdata/user/dev/flink-$flink_version-dependency/plugins" \
-Dyarn.application.name=ods_etl_batch_job \
klrtdw-1.0.jar \
--database tmp \
--limit 1000000