package hikaricp.jdbc.poc;

import hikaricp.jdbc.poc.models.Address;
import hikaricp.jdbc.poc.models.Person;
import hikaricp.jdbc.poc.persistence.Dao;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class App {

  private static void initDb() throws SQLException {
    try (Connection conn = DriverManager.getConnection(Dao.DB_URL, Dao.DB_USER, Dao.DB_PASS)) {
      Dao.createTable(Dao.CREATE_TABLE_ADDRESS, conn);
      Dao.createTable(Dao.CREATE_TABLE_PERSON, conn);
      System.out.println("DB init");
    }
  }

  public static void main(String[] args) throws SQLException {
    initDb();
    runDirectConnection();

    // singleton inject into service
    Dao dao = new Dao();
    run(dao);
    query(dao);

    dao.close();
  }

  private static void runDirectConnection() throws SQLException {
    try (Connection conn = DriverManager.getConnection(Dao.DB_URL, Dao.DB_USER, Dao.DB_PASS)) {
      run(conn);
      System.out.println("Done Direct Conn ...");
    }
  }

  // business logic is aware of connection
  private static void run(Connection connection) {
    Address address = new Address(100, "15 Fake st", "Nowhere");
    Person groom = new Person(1, "John Smith", 100);
    Person bride = new Person(2, "Jane Doe", 100);

    try {
      Dao.upsertAddress(address, connection);
      Dao.upsertPerson(groom, connection);
      Dao.upsertPerson(bride, connection);
    } catch (SQLException e) {
      throw new RuntimeException("Caught while doing business logic", e);
    }
  }

  private static void run(Dao dao) throws SQLException {
    dao.doWork(App::run);
    System.out.println("Done Dao ...");
  }

  private static void query(Dao dao) throws SQLException {
    dao.doWork(
        conn -> {
          try {
            System.out.println(Dao.getPersonDetails(1, conn));
            System.out.println(Dao.getPersonDetails(2, conn));
          } catch (SQLException e) {
            throw new RuntimeException("Caught while querying business logic", e);
          }
        });
  }
}
