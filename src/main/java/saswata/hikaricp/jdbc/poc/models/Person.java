package saswata.hikaricp.jdbc.poc.models;

public class Person {
  public final long id;
  public final String name;
  public final long addressId; // FK

  public Person(long id, String name, long addressId) {
    this.id = id;
    this.name = name;
    this.addressId = addressId;
  }

  @Override
  public String toString() {
    return "Person{" + "id=" + id + ", name='" + name + '\'' + ", addressId=" + addressId + '}';
  }
}
