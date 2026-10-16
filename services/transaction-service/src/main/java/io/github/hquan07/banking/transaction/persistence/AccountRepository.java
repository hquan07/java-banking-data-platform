package io.github.hquan07.banking.transaction.persistence;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class AccountRepository {

    private final JdbcTemplate jdbcTemplate;

    public AccountRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<AccountRecord> findById(String accountId) {
        return jdbcTemplate.query(
                "SELECT account_id, currency, status FROM accounts WHERE account_id = ?",
                (resultSet, rowNumber) -> new AccountRecord(
                        resultSet.getString("account_id"),
                        resultSet.getString("currency").trim(),
                        resultSet.getString("status")
                ),
                accountId
        ).stream().findFirst();
    }
}
