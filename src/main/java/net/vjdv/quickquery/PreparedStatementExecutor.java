package net.vjdv.quickquery;

import net.vjdv.quickquery.exceptions.QueryException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public class PreparedStatementExecutor<T> {
    private final PreparedStatement stmt;
    private final Function<ResultSetWrapper, T> function;

    public PreparedStatementExecutor(PreparedStatement stmt, Function<ResultSetWrapper, T> function) {
        this.stmt = stmt;
        this.function = function;
    }

    public Optional<T> findOne() {
        AtomicReference<T> result = new AtomicReference<>(null);
        try (stmt; var rs = stmt.executeQuery()) {
            if (rs.next()) {
                T item = function.apply(new ResultSetWrapper(rs));
                result.set(item);
            }
        } catch (SQLException ex) {
            throw new QueryException("Error quering item", ex);
        }
        return Optional.ofNullable(result.get());
    }

    public List<T> list() {
        List<T> list = new ArrayList<>();
        try (stmt; var rs = stmt.executeQuery()) {
            while (rs.next()) {
                T item = function.apply(new ResultSetWrapper(rs));
                list.add(item);
            }
        } catch (SQLException ex) {
            throw new QueryException("Error quering list", ex);
        }
        return list;
    }

    public void forEach(Consumer<T> consumer) {
        try (stmt; var rs = stmt.executeQuery()) {
            while (rs.next()) {
                T item = function.apply(new ResultSetWrapper(rs));
                consumer.accept(item);
            }
        } catch (SQLException ex) {
            throw new QueryException("Error quering list", ex);
        }
    }

}
