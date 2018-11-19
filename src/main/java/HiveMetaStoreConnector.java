import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TException;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;

public class HiveMetaStoreConnector {
    private HiveConf hiveConf;
    HiveMetaStoreClient hiveMetaStoreClient;

    public HiveMetaStoreConnector(String msAddr, String msPort) {
        try {
            hiveConf = new HiveConf();
            hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, msAddr + ":" + msPort);
            hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            e.printStackTrace();
            System.err.println("Constructor error");
            System.err.println(e.toString());
            System.exit(-100);
        }
    }

    public HiveMetaStoreConnector(HiveConf hiveConf) {
        try {
            this.hiveConf = hiveConf;
            hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            e.printStackTrace();
            System.err.println("Constructor error");
            System.err.println(e.toString());
            System.exit(-100);
        }
    }

    public String getAllPartitionInfo(String dbName) {
        List<String> res = Lists.newArrayList();
        try {
            List<String> tableList = hiveMetaStoreClient.getAllTables(dbName);
            for (String tableName : tableList) {
                res.addAll(getTablePartitionInformation(dbName, tableName));
            }
        } catch (MetaException e) {
            e.printStackTrace();
            System.out.println("getAllTableStatistic error");
            System.out.println(e.toString());
            System.exit(-100);
        }

        return Joiner.on("\n").join(res);
    }

    public List<String> getTablePartitionInformation(String dbName, String tableName) {
        List<String> partitionsInfo = Lists.newArrayList();
        try {
            List<String> partitionNames = hiveMetaStoreClient.listPartitionNames(dbName, tableName, (short) 10000);
            List<Partition> partitions = hiveMetaStoreClient.listPartitions(dbName, tableName, (short) 10000);
            for (Partition partition : partitions) {
                StringBuffer sb = new StringBuffer();
                sb.append(tableName);
                sb.append("\t");
                List<String> partitionValues = partition.getValues();
                if (partitionValues.size() < 4) {
                    int size = partitionValues.size();
                    for (int j = 0; j < 4 - size; j++) {
                        partitionValues.add("null");
                    }
                }
                sb.append(Joiner.on("\t").join(partitionValues));
                sb.append("\t");
                DateTime createDate = new DateTime((long) partition.getCreateTime() * 1000);
                sb.append(createDate.toString("yyyy-MM-dd HH:mm:ss"));
                partitionsInfo.add(sb.toString());
            }

        } catch (TException e) {
            e.printStackTrace();
            return Arrays.asList(new String[]{"error for request on" + tableName});
        }

        return partitionsInfo;
    }

    public String getAllTableStatistic(String dbName) {
        List<String> res = Lists.newArrayList();
        try {
            List<String> tableList = hiveMetaStoreClient.getAllTables(dbName);
            for (String tableName : tableList) {
                res.addAll(getTableColumnsInformation(dbName, tableName));
            }
        } catch (MetaException e) {
            e.printStackTrace();
            System.out.println("getAllTableStatistic error");
            System.out.println(e.toString());
            System.exit(-100);
        }

        return Joiner.on("\n").join(res);
    }

    public List<String> getTableColumnsInformation(String dbName, String tableName) {
        try {
            List<FieldSchema> fields = hiveMetaStoreClient.getFields(dbName, tableName);
            List<String> infs = Lists.newArrayList();
            int cnt = 0;
            for (FieldSchema fs : fields) {
                StringBuffer sb = new StringBuffer();
                sb.append(tableName);
                sb.append("\t");
                sb.append(cnt);
                sb.append("\t");
                cnt++;
                sb.append(fs.getName());
                sb.append("\t");
                sb.append(fs.getType());
                sb.append("\t");
                sb.append(fs.getComment());
                infs.add(sb.toString());
            }

            return infs;

        } catch (TException e) {
            e.printStackTrace();
            System.out.println("getTableColumnsInformation error");
            System.out.println(e.toString());
            System.exit(-100);
            return null;
        }
    }
}
