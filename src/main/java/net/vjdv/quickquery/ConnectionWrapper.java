package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.DataAccessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public interface ConnectionWrapper {

    Connection getConnection();

    /**
     * Creates a new PreparedStatementBuilder for a query
     *
     * @param sql query
     * @return PreparedStatementBuilder
     */
    default PreparedStatementBuilder query(String sql) {
        var conn = getConnection();
        try {
            var stmt = conn.prepareStatement(sql);
            return new PreparedStatementBuilder(conn, stmt);
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
    default PreparedStatementBuilder queryWithGeneratedKey(String sql) {
        var conn = getConnection();
        try {
            var stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            return new PreparedStatementBuilder(conn, stmt);
        } catch (SQLException ex) {
            throw new DataAccessException("Error creating prepared statement", ex);
        }
    }

}
