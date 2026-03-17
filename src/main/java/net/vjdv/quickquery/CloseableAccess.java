package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.DataAccessException;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;

public class CloseableAccess implements ConnectionWrapper, Closeable {
    private final Connection connection;

    public CloseableAccess(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public void close() {
        try {
            this.connection.close();
        } catch (SQLException ex) {
            throw new DataAccessException("Failed to close connection", ex);
        }
    }

}
