# Quick Query

When working on a small project, adding a heavy framework
just to manage database queries might feel excessive.
You may want to avoid creating multiple files for simple queries
or prefer writing your own queries with minimal configuration.

However, vanilla `java.sql` syntax can be verbose and cumbersome.
This is where *QuickQuery* comes in offering a lightweight and simple way
to handle database queries with ease.

Available on Maven Central:

```xml
<dependency>
    <groupId>net.vjdv</groupId>
    <artifactId>quickquery</artifactId>
    <version>1.1.3</version>
</dependency>
```

## Examples

Get one result with vanilla `java.sql`:

```java
public Person getPerson(int id) {
    try (PreparedStatement stmt = connection.prepareStatement("SELECT * FROM person WHERE id = ?")) {
        stmt.setInt(1, id);
        try (ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return new Person(rs.getInt("id"), rs.getString("name"));
            }
        }
    } catch (SQLException ex) {
        throw new RuntimeException(ex);
    }
    return null;
}
```

With *QuickQuery*:

```java
public Optional<Person> getPerson(int id) {
    dataAccess.query("SELECT * FROM person WHERE id = ?")
              .setInt(id)
              .resultMapper(rs -> new Person(rs.getInt("id"), rs.getString("name")))
              .findOne();
}
```

Get a list of results with vanilla `java.sql`:

```java
public List<Person> getPeople() {
    List<Person> people = new ArrayList<>();
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM person")) {
        while (rs.next()) {
            people.add(new Person(rs.getInt("id"), rs.getString("name")));
        }
    } catch (SQLException ex) {
        throw new RuntimeException(ex);
    }
    return people;
}
```

With *QuickQuery*:

```java
public List<Person> getPeople() {
    return dataAccess.query("SELECT * FROM person")
                      .resultMapper(rs -> new Person(rs.getInt("id"), rs.getString("name")))
                      .list();
}
```

## Usage

*QuickQuery* requires a connection supplier to create a DataAccess object.
The connection supplier can be a DataSource (recommended)
or you can use an existing connection.

Example with [HikariCP](https://github.com/brettwooldridge/HikariCP):

```java
import net.vjdv.quickquery.QuickQuery;

HikariDataSource ds = new HikariDataSource(config);
DataAccess dataAccess = QuickQuery.fromDataSource(ds);
```

Single connection example with sqlite:

```java
Connection connection = DriverManager.getConnection("jdbc:sqlite:database.db");
DataAccess dataAccess = QuickQuery.fromConnection(connection);
```

If you use *Spring Boot*, you can create a `@Bean` for the `DataAccess` object:

```java
@Bean
public DataAccess dataAccess(DataSource dataSource) {
    return QuickQuery.fromDataSource(dataSource);
}
```

And then inject it into your services:

```java
@Service
public class PersonService {

    private final DataAccess dataAccess;

    public PersonService(DataAccess dataAccess) {
        this.dataAccess = dataAccess;
    }

    public Optional<Person> getPerson(int id) {
        return dataAccess.query("SELECT * FROM person WHERE id = ?")
                          .setInt(id)
                          .resultMapper(rs -> new Person(rs.getInt("id"), rs.getString("name")))
                          .findOne();
    }

    public List<Person> getPeople() {
        return dataAccess.query("SELECT * FROM person")
                          .resultMapper(rs -> new Person(rs.getInt("id"), rs.getString("name")))
                          .list();
    }
}
```

A `DataAccessException` is thrown for any SQL exception that occurs during query execution.
It extends `RuntimeException`, don't forget to handle it!
For example with *Spring* you can add an `@ExceptionHandler` in your controllers:

```java
@ExceptionHandler(DataAccessException.class)
public ResponseEntity<String> handleDataAccessException(DataAccessException ex) {
    log.error("Database error in web", ex);
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Database error");
}
```
