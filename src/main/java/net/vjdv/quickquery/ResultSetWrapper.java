package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.QueryException;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ResultSetWrapper {
    private final ResultSet rs;

    public ResultSetWrapper(ResultSet rs) {
        this.rs = rs;
    }

    public boolean next() {
        try {
            return rs.next();
        } catch (SQLException ex) {
            throw new QueryException("Error getting next result", ex);
        }
    }

    public byte[] getBytes(String column) {
        try {
            return rs.getBytes(column);
        } catch (SQLException ex) {
            throw new QueryException("Error getting bytes from column " + column, ex);
        }
    }

    public byte[] getBytes(int column) {
        try {
            return rs.getBytes(column);
        } catch (SQLException ex) {
            throw new QueryException("Error getting bytes from column " + column, ex);
        }
    }

    public int getInt(String column) {
        try {
            return rs.getInt(column);
        } catch (SQLException ex) {
            throw new QueryException("Error getting int from column " + column, ex);
        }
    }

    public int getInt(int column) {
        try {
            return rs.getInt(column);
        } catch (SQLException ex) {
            throw new QueryException("Error getting int from column " + column, ex);
        }
    }

    public String getString(String column) {
        try {
            return rs.getString(column);
        } catch (SQLException ex) {
            throw new QueryException("Error getting string from column " + column, ex);
        }
    }

    public String getString(int column) {
        try {
            return rs.getString(column);
        } catch (SQLException ex) {
            throw new QueryException("Error getting string from column " + column, ex);
        }
    }

    public long getLong(String column) {
        try {
            return rs.getLong(column);
        } catch (SQLException ex) {
            throw new QueryException("Error getting long from column " + column, ex);
        }
    }

    public long getLong(int column) {
        try {
            return rs.getLong(column);
        } catch (SQLException ex) {
            throw new QueryException("Error getting long from column " + column, ex);
        }
    }
}
