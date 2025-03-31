package net.vjdv;

import net.vjdv.quickquery.DataAccess;
import net.vjdv.quickquery.QuickQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.LocalDateTime;

/**
 * Unit test for simple App.
 */
public class AppTest {
    private static final DataAccess data;

    static {
        //sqlite database
        String url = "jdbc:sqlite:" + Path.of("db.sqlite").toAbsolutePath();
        data = QuickQuery.createConnection("org.sqlite.JDBC", url);
        //Create table
        String sql = """
                CREATE TABLE IF NOT EXISTS person (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    age INTEGER
                )""";
        data.query(sql).execute();
        //Another table
        sql = """
                CREATE TABLE IF NOT EXISTS point (
                    id BLOB PRIMARY KEY,
                    quantity INTEGER,
                    pos REAL,
                    datetime INTEGER
                )""";
        data.query(sql).execute();
    }

    @Test
    public void testInsertAndFindOne() {
        data.query("INSERT INTO person (name, age) VALUES ('John', 25)").execute();
        var person1 = data.query("SELECT name, age FROM person WHERE id = 1")
                .resultMapper(rs -> new Person(rs.getString("name"), rs.getInt("age")))
                .findOne();
        Assertions.assertTrue(person1.isPresent());
        Assertions.assertEquals("John", person1.get().name);
        Assertions.assertEquals(25, person1.get().age);
    }

    @Test
    public void testInsertAndList() {
        data.query("INSERT INTO person (name, age) VALUES ('John', 25)").execute();
        data.query("INSERT INTO person (name, age) VALUES ('Jane', 30)").execute();
        var people = data.query("SELECT name, age FROM person")
                .resultMapper(rs -> new Person(rs.getString("name"), rs.getInt("age")))
                .list();
        Assertions.assertFalse(people.isEmpty());
        Assertions.assertTrue(people.size() >= 2);
    }

    @Test
    public void testInsertValues() {
        String sql = "INSERT INTO person (name, age) VALUES (?, ?), (?, ?)";
        int affected = data.query(sql)
                .setString("John")
                .setInt(25)
                .setString("Jane")
                .setInt(30)
                .executeUpdate();
        Assertions.assertEquals(2, affected);
    }

    @Test
    public void testPoint() {
        var id = new byte[16];
        new SecureRandom().nextBytes(id);
        var now = LocalDateTime.parse("2025-03-26T21:30:05.123");
        var point = new Point(id, 100, 3.14, now);
        data.query("INSERT INTO point (id, quantity, pos, datetime) VALUES (?, ?, ?, ?)")
                .setBytes(point.id)
                .setInt(point.quantity)
                .setDouble(point.pos)
                .setLocalDateTimeLong(point.datetime)
                .execute();
        var point1 = data.query("SELECT id, quantity, pos, datetime FROM point WHERE id = ?")
                .setBytes(point.id)
                .resultMapper(rs -> {
                    byte[] id2 = rs.getBytes("id");
                    int quantity = rs.getInt("quantity");
                    double pos = rs.getDouble("pos");
                    LocalDateTime datetime = rs.getLocalDateTimeLong("datetime");
                    return new Point(id2, quantity, pos, datetime);
                })
                .findOne();
        Assertions.assertTrue(point1.isPresent());
        Assertions.assertArrayEquals(point.id, point1.get().id);
        Assertions.assertEquals(point.quantity, point1.get().quantity);
        Assertions.assertEquals(point.pos, point1.get().pos);
        Assertions.assertEquals(point.datetime, point1.get().datetime);
    }

    @Test
    public void testExecuteQuery() {
        data.query("INSERT INTO person (name, age) VALUES ('Will', 90), ('Sam', 90)").execute();
        data.query("SELECT name, age FROM person WHERE age = ?")
                .setInt(90)
                .executeQuery()
                .forEach(rs -> {
                    String name = rs.getString("name");
                    int age = rs.getInt("age");
                    Assertions.assertNotNull(name);
                    Assertions.assertEquals(90, age);
                });
    }

    record Person(String name, int age) {
    }

    record Point(byte[] id, int quantity, double pos, LocalDateTime datetime) {
    }
}
