package net.vjdv;

import net.vjdv.quickquery.DataAccess;
import net.vjdv.quickquery.exceptions.DataAccessException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Unit test for simple App.
 */
public class AppTest {
    private static final DataAccess data;

    static {
        //sqlite database
        String url = "jdbc:sqlite:" + Path.of("db.sqlite").toAbsolutePath();
        try {
            Class.forName("org.sqlite.JDBC");
            var connection = DriverManager.getConnection(url);
            data = new DataAccess(connection);
            //Create table
            String sql = """
                    CREATE TABLE IF NOT EXISTS person (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT,
                        age INTEGER
                    )""";
            data.query(sql).execute();
        } catch (ClassNotFoundException | SQLException ex) {
            throw new DataAccessException("Error opening database", ex);
        }
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

    record Person(String name, int age) {
    }
}
