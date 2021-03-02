package hikaricp.jdbc.poc.models;

public class Address {
  public final long id;
  public final String line1;
  public final String line2;

  public Address(long id, String line1, String line2) {
    this.id = id;
    this.line1 = line1;
    this.line2 = line2;
  }

  @Override
  public String toString() {
    return "Address{" + "id=" + id + ", line1='" + line1 + '\'' + ", line2='" + line2 + '\'' + '}';
  }
}
