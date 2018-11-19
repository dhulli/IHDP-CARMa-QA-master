import org.apache.hadoop.hive.conf.HiveConf;

// Test program
public class PolicyHandler {
    public static void main(String[] args){

        HiveConf hiveConf = new HiveConf();
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://host:port");

        HiveMetaStoreConnector hiveMetaStoreConnector = new HiveMetaStoreConnector(hiveConf);
        if(hiveMetaStoreConnector != null){
            System.out.print(hiveMetaStoreConnector.getAllPartitionInfo("tablename"));
        }
    }
}