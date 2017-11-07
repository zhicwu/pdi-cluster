package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Cassandra database.
 *
 * @author zhicwu
 * @since Nov 2, 2017
 */
public class CassandraDialect extends JdbcDialectImpl {
    public static final JdbcDialectFactory FACTORY =
            new JdbcDialectFactory(
                    CassandraDialect.class,
                    DatabaseProduct.CASSANDRA);

    /**
     * Creates an IngresDialect.
     *
     * @param connection Connection
     */
    public CassandraDialect(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    public boolean allowsJoinOn() {
        return false;
    }

    @Override
    public boolean allowsSelectNotInGroupBy() {
        return false;
    }

    @Override
    public boolean allowsCompoundCountDistinct() {
        return false;
    }
}
