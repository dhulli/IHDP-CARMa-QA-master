import java.sql.*;

public class HiveSchemaQuery {
    // JDBC driver name and Hive Database URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:hive2://10.224.150.145:10000/default";

    //  Hive Database credentials
    static final String USER = "hivetest";
    static final String PASS = "hivetest";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try{
            //STEP 2: Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");

            //STEP 3: Open a connection
            System.out.println("Connecting to a selected database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected database successfully...");

            //STEP 4: Execute a query
            System.out.println("Creating statement...");
            stmt = conn.createStatement();

            String sql = "PAYLOAD_CONTAINER_NAME, PAYLOAD_CONTAINER_SCHEMA, PAYLOAD_CONTAINER_FORMAT, PAYLOAD_CONTAINER_VERSION	PAYLOAD_CONTAINER_YEAR, " +
                    "PAYLOAD_CONTAINER_MONTH, PAYLOAD_CONTAINER_DAY	LINE_IND, claim_tcn_id, service_line_item_number, revenue_code, procedure_code_qualifier, " +
                    "procedure_code	procedure_modifier_1, procedure_modifier_2, procedure_modifier_3, procedure_modifier_4, service_line_charge_amount, service_unit_of_measure	service_unit_count," +
                    "service_facility_code, diagnosis_code_pointer_1, diagnosis_code_pointer_2, diagnosis_code_pointer_3, diagnosis_code_pointer_4, emergency_indicator, dme_length_of_med_necessity, " +
                    "dme_rental_price, dme_purchase_price, dme_rental_unit_price_ind, dme_procedure_code, service_from_date, service_to_date, prescription_national_drug_cd, prescription_units, " +
                    "prescription_unit_of_measure, referring_prov_org_last_name, referring_prov_first_name, referring_prov_npi, rendering_prov_org_last_name, rendering_prov_first_name," +
                    "rendering_prov_npi, lab_or_facility_org_name, lab_or_facility_npi, supervising_prov_last_name, supervising_prov_first_name, supervising_prov_npi, operating_prov_last_name, " +
                    "operating_prov_first_name, operating_prov_npi, ordering_prov_last_name, ordering_prov_first_name, ordering_prov_npi, purchased_service_prov_npi, prior_authorization_ind, " +
                    "denied_noncovered_charge_amt	prior_authorization_payer_id, service_location_org_name	service_location_npi, fileName, customer, accessType, businessTransactionTypeFormat, " +
                    "contract, dataCategory, datasource, guid, id, metadata, name, security, sourceTransactionFormat";
            ResultSet rs = stmt.executeQuery(sql);
            //STEP 5: Extract data from result set
            while(rs.next()){
                //Retrieve by column name
                int id  = rs.getInt("id");
                int age = rs.getInt("age");
                String first = rs.getString("first");
                String last = rs.getString("last");

                //Display values
                System.out.print("ID: " + id);
                System.out.print(", Age: " + age);
                System.out.print(", First: " + first);
                System.out.println(", Last: " + last);
            }
            rs.close();
        }catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
        }catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        }finally{
            //finally block used to close resources
            try{
                if(stmt!=null)
                    conn.close();
            }catch(SQLException se){
            }// do nothing
            try{
                if(conn!=null)
                    conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }//end finally try
        }//end try
        System.out.println("Goodbye!");
    }//end main
}//end JDBCExample