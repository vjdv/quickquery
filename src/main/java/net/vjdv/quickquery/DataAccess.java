package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.DataAccessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Supplier;

/**
 * Data access class
 */
public class DataAccess {
    private final Supplier<Connection> connectionSupplier;
    private final boolean closeConnection;

    /**
     * Creates a new instance of DataAccess
     *
     * @param connectionSupplier supplier of connection
     */
    protected DataAccess(Supplier<Connection> connectionSupplier, boolean closeConnection) {
        this.connectionSupplier = connectionSupplier;
        this.closeConnection = closeConnection;
    }

    /**
     * Connection shorter
     *
     * @return connection
     */
    private Connection conn() {
        return connectionSupplier.get();
    }

    /**
     * Creates a new PreparedStatementBuilder for a query
     *
     * @param sql query
     * @return PreparedStatementBuilder
     */
    public PreparedStatementBuilder query(String sql) {
        try {
            var stmt = conn().prepareStatement(sql);
            return new PreparedStatementBuilder(new PreparedStatementWrapper(stmt, closeConnection));
        } catch (SQLException ex) {
            throw new DataAccessException("Error creating prepared statement", ex);
        }
    }

    /**
     * Creates a new PreparedStatementBuilder for a query with generated keys, useful for autoincrement or serial columns
     *
     * @param sql query
     * @return PreparedStatementBuilder
     */
    public PreparedStatementBuilder queryWithGeneratedKey(String sql) {
        try {
            var stmt = conn().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            return new PreparedStatementBuilder(new PreparedStatementWrapper(stmt, closeConnection));
        } catch (SQLException ex) {
            throw new DataAccessException("Error creating prepared statement", ex);
        }
    }
}
