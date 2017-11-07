package mondrian.spi.impl;

import mondrian.spi.Dialect;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the ClickHouse database.
 *
 * @author zhicwu
 * @since Nov 2, 2017
 */
public class ClickHouseDialect extends JdbcDialectImpl {
    public static final JdbcDialectFactory FACTORY =
            new JdbcDialectFactory(
                    ClickHouseDialect.class,
                    DatabaseProduct.CLICKHOUSE);

    /**
     * Creates an IngresDialect.
     *
     * @param connection Connection
     */
    public ClickHouseDialect(Connection connection) throws SQLException {
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
}
