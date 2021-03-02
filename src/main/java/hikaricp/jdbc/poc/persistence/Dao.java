package hikaricp.jdbc.poc.persistence;

import hikaricp.jdbc.poc.models.Address;
import hikaricp.jdbc.poc.models.Person;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.function.Consumer;

public class Dao {
  public static final String DB_URL = "jdbc:hsqldb:mem:MYDB";
  public static final String DB_USER = "SA";
  public static final String DB_PASS = "";

  private final Connection connection;

  public Dao() throws SQLException {
    connection = DriverManager.getConnection(Dao.DB_URL, Dao.DB_USER, Dao.DB_PASS);
  }

  public void doWork(Consumer<Connection> work) throws SQLException {
    try {
      connection.setAutoCommit(false);
      work.accept(connection);
      connection.commit();
    } catch (Exception e) {
      System.err.println("Error " + e.getMessage());
      e.printStackTrace();

      connection.rollback();
    }
  }

  public void close() throws SQLException {
    connection.close();
  }

  private static final String PERSON_MERGE_SQL =
      "MERGE INTO PERSON as p \n"
          + "USING (VALUES(?, ?, ?)) AS vals(id, name, addressId)\n"
          + "ON p.id = vals.id\n"
          + "WHEN MATCHED THEN UPDATE SET p.name = vals.name, p.addressId = vals.addressId\n"
          + "WHEN NOT MATCHED THEN INSERT VALUES vals.id, vals.name, vals.addressId\n";

  public static void upsertPerson(Person person, Connection connection) throws SQLException {
    try (PreparedStatement pstmt = connection.prepareStatement(PERSON_MERGE_SQL)) {
      pstmt.setLong(1, person.id);
      pstmt.setString(2, person.name);
      pstmt.setLong(3, person.addressId);
      int rowCount = pstmt.executeUpdate();

      assert rowCount == 1 : String.format("Bad row count = %d for Person = %s", rowCount, person);
    }
  }

  private static final String ADDRESS_MERGE_SQL =
      "MERGE INTO ADDRESS as a\n"
          + "USING (VALUES(?, ?, ?)) AS vals(id, line1, line2)\n"
          + "ON a.id = vals.id\n"
          + "WHEN MATCHED THEN UPDATE SET a.line1 = vals.line1, a.line2 = vals.line2\n"
          + "WHEN NOT MATCHED THEN INSERT VALUES vals.id, vals.line1, vals.line1";

  public static void upsertAddress(Address address, Connection connection) throws SQLException {
    try (PreparedStatement pstmt = connection.prepareStatement(ADDRESS_MERGE_SQL)) {
      pstmt.setLong(1, address.id);
      pstmt.setString(2, address.line1);
      pstmt.setString(3, address.line2);
      int rowCount = pstmt.executeUpdate();

      assert rowCount == 1
          : String.format("Bad row count = %d for Address = %s", rowCount, address);
    }
  }

  public static final String CREATE_TABLE_PERSON =
      "CREATE TABLE PERSON(id BIGINT NOT NULL, name CHAR(10), addressId BIGINT,"
          + " PRIMARY KEY (id),"
          + " FOREIGN KEY (addressId) REFERENCES ADDRESS(id))";

  public static final String CREATE_TABLE_ADDRESS =
      "CREATE TABLE ADDRESS(id BIGINT NOT NULL, line1 CHAR(10), line2 CHAR(10),"
          + " PRIMARY KEY (id))";

  public static void createTable(String sql, Connection connection) throws SQLException {
    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
      int rowCount = pstmt.executeUpdate();
      assert rowCount == 1
          : String.format("Bad row count = %d for CREATE_TABLE = %s", rowCount, sql);
    }
  }

  private static final String PERSON_QUERY =
      //      "SELECT * from PERSON WHERE id = ?";
      "SELECT p.id, name, a.id, line1, line2"
          + " from PERSON as p"
          + " join ADDRESS as a"
          + " on a.id = p.addressId AND p.id = ?";

  public static String getPersonDetails(long pid, Connection connection) throws SQLException {
    try (PreparedStatement pstmt = connection.prepareStatement(PERSON_QUERY)) {

      pstmt.setLong(1, pid);
      ArrayList<String> response = new ArrayList<>();

      try (ResultSet resultSet = pstmt.executeQuery()) {
        while (resultSet.next()) {
          response.add(resultSet.getString(1)); // p.id
          response.add(resultSet.getString(2)); // name
          response.add(resultSet.getString(3)); // add Id
          response.add(resultSet.getString(4)); // line 1
          response.add(resultSet.getString(5)); // line 2
        }
      }

      return response.toString();
    }
  }
}
