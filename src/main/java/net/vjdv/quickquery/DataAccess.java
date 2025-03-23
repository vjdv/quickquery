package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.DataAccessException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Supplier;

public class DataAccess {
    private final Supplier<Connection> connectionSupplier;

    public DataAccess(Supplier<Connection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    public DataAccess(Connection conn) {
        this(() -> conn);
    }

    public DataAccess(DataSource dataSource) {
        this(() -> {
            try {
                return dataSource.getConnection();
            } catch (SQLException ex) {
                throw new DataAccessException("Error getting connection from datasource", ex);
            }
        });
    }

    private Connection conn() {
        return connectionSupplier.get();
    }

    public PreparedStatementBuilder query(String sql) {
        try {
            var stmt = conn().prepareStatement(sql);
            return new PreparedStatementBuilder(stmt);
        } catch (SQLException ex) {
            throw new DataAccessException("Error creating prepared statement", ex);
        }
    }

    public PreparedStatementBuilder queryWithGeneratedKey(String sql) {
        try {
            var stmt = conn().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            return new PreparedStatementBuilder(stmt);
        } catch (SQLException ex) {
            throw new DataAccessException("Error creating prepared statement", ex);
        }
    }
}
