import org.apache.hive.jdbc.HiveBaseResultSet;
import java.sql.*;


public class HiveJdbcClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }

        //set the Hive connection using URL,user, password credentials
        Connection con = DriverManager.getConnection("jdbc:hive2://10.224.154.176:10000/default?hive.execution.engine=tez", "hivetest", "hivetest");
        Statement stmt = con.createStatement();

       // show tables on Hive Catalog
        String sql = "show tables";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }

        //Hive Query which executes clm_claim_deid table
        String sql1 = "select claim_tcn_id FROM clm_claim_deid\n" +
                "   limit 10";
        System.out.println("SQL1 = " + sql1);
        ResultSet res1 = stmt.executeQuery(sql1);
        System.out.println("Test");
        while (res1.next()) {
            System.out.println(res1.getString(1));
        }

        // Hive Query to display filename which limit to 10 from a specific deid table
        String sql2 = "select filename " +
                "FROM clm_claim_deid " +
                "limit 10";
        System.out.println("SQL2 = " + sql2);
        ResultSet res2 = stmt.executeQuery(sql2);
        System.out.println("Test after result set");
        while (res2.next()) {
            System.out.println(res2.getString(1));
        }

        //Hive Query to retrieve businessTransactionTypeFormat from a deid table
        String sql3 = "select businessTransactionTypeFormat FROM clm_claim_deid limit 5";
        System.out.println("SQL3 = " + sql3);
        ResultSet res3 = stmt.executeQuery(sql3);
        System.out.println("Test after result set");
        while (res3.next()) {
            System.out.println(res3.getString(1));
        }

        //Hive Query to retrieves accesstype pou's in clm_claim_deid table
        String sql4 = "select accesstype.pou FROM clm_claim_deid limit 1";
        System.out.println("SQL 4 = " + sql4);
        HiveBaseResultSet  res4 = (HiveBaseResultSet)stmt.executeQuery(sql4);
        System.out.println("Test after result set");
        while (res4.next()) {
            System.out.println(res4.getString(1));
        }

        //Hive Query to retrieves security banno's from a clm_claim_deid table
        String sql5 = "select security.baano FROM clm_claim_deid limit 5";
        System.out.println("SQL3 = " + sql5);
        ResultSet res5 = stmt.executeQuery(sql5);
        System.out.println("Test after result set");
        while (res5.next()) {
            System.out.println(res5.getString(1));
        }
    }
}

