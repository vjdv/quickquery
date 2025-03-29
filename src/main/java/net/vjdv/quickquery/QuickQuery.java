package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.DataAccessException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.function.Supplier;

/**
 * QuickQuery for building a DataAccess instance
 */
public class QuickQuery {
    private QuickQuery() {
    }

    /**
     * Creates a new instance of DataAccess from a connection supplier
     *
     * @param connectionSupplier connection supplier
     * @return DataAccess instance
     */
    public static DataAccess fromSupplier(Supplier<Connection> connectionSupplier) {
        return new DataAccess(connectionSupplier);
    }

    /**
     * Creates a new instance of DataAccess from an existing connection
     *
     * @param connection connection
     * @return DataAccess instance
     */
    public static DataAccess fromConnection(Connection connection) {
        return fromSupplier(() -> connection);
    }

    /**
     * Creates a new instance of DataAccess from a DataSource
     *
     * @param dataSource DataSource
     * @return DataAccess instance
     * @throws DataAccessException if there is an error getting the connection
     */
    public static DataAccess fromDataSource(DataSource dataSource) {
        return new DataAccess(() -> {
            try {
                return dataSource.getConnection();
            } catch (SQLException ex) {
                throw new DataAccessException("Error getting connection from datasource", ex);
            }
        });
    }

    /**
     * Creates a new instance of DataAccess creating a connection
     *
     * @param driver driver class name
     * @param url    connection url
     * @return DataAccess instance
     */
    public static DataAccess createConnection(String driver, String url) {
        try {
            Class.forName(driver);
            var connection = DriverManager.getConnection(url);
            return fromConnection(connection);
        } catch (ClassNotFoundException | SQLException ex) {
            throw new DataAccessException("Error opening database", ex);
        }
    }
}
