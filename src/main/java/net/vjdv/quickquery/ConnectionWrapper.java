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

    /**
     * Enables auto-commit mode for the connection. In auto-commit mode, each individual SQL statement is treated as a transaction and is automatically committed after it is executed. This means that if you execute a statement that modifies the database, the changes will be immediately saved without needing to call commit() explicitly.
     */
    default void enableAutoCommit() {
        try {
            getConnection().setAutoCommit(true);
        } catch (SQLException ex) {
            throw new DataAccessException("Error enabling auto-commit", ex);
        }
    }

    /**
     * Disables auto-commit mode for the connection. When auto-commit is disabled, you need to explicitly call commit() to save changes to the database. This allows you to group multiple SQL statements into a single transaction, which can be rolled back if any of the statements fail. Disabling auto-commit is useful when you want to ensure that a series of related operations either all succeed or all fail together, maintaining data integrity.
     */
    default void disableAutoCommit() {
        try {
            getConnection().setAutoCommit(false);
        } catch (SQLException ex) {
            throw new DataAccessException("Error disabling auto-commit", ex);
        }
    }

    /**
     * Commits the current transaction
     */
    default void commit() {
        try {
            getConnection().commit();
        } catch (SQLException ex) {
            throw new DataAccessException("Error committing transaction", ex);
        }
    }

    /**
     * Rolls back the current transaction
     */
    default void rollback() {
        try {
            getConnection().rollback();
        } catch (SQLException ex) {
            throw new DataAccessException("Error rolling back transaction", ex);
        }
    }

}
