package io.github.hquan07.banking.transaction.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hquan07.banking.common.event.TransactionCreatedEvent;
import io.github.hquan07.banking.common.event.TransactionStatus;
import io.github.hquan07.banking.transaction.api.TransactionRequest;
import io.github.hquan07.banking.transaction.persistence.AccountRecord;
import io.github.hquan07.banking.transaction.persistence.AccountRepository;
import io.github.hquan07.banking.transaction.persistence.OutboxRepository;
import io.github.hquan07.banking.transaction.persistence.StoredTransaction;
import io.github.hquan07.banking.transaction.persistence.TransactionRepository;
import io.github.hquan07.banking.transaction.support.AccountNotFoundException;
import io.github.hquan07.banking.transaction.support.AccountStateConflictException;
import io.github.hquan07.banking.transaction.support.IdempotencyConflictException;
import io.github.hquan07.banking.transaction.support.TransactionNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

@Service
public class TransactionApplicationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionApplicationService.class);

    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;
    private final OutboxRepository outboxRepository;
    private final RequestFingerprint requestFingerprint;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public TransactionApplicationService(
            AccountRepository accountRepository,
            TransactionRepository transactionRepository,
            OutboxRepository outboxRepository,
            RequestFingerprint requestFingerprint,
            ObjectMapper objectMapper,
            Clock clock
    ) {
        this.accountRepository = accountRepository;
        this.transactionRepository = transactionRepository;
        this.outboxRepository = outboxRepository;
        this.requestFingerprint = requestFingerprint;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @Transactional
    public CreateTransactionResult create(
            TransactionRequest request,
            String idempotencyKey,
            String correlationId
    ) {
        String fingerprint = requestFingerprint.calculate(request);
        transactionRepository.lockIdempotencyKey(idempotencyKey);

        var existing = transactionRepository.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            if (!existing.get().requestFingerprint().equals(fingerprint)) {
                throw new IdempotencyConflictException();
            }
            LOGGER.atInfo()
                    .addKeyValue("transaction_id", existing.get().transactionId())
                    .addKeyValue("idempotent_replay", true)
                    .log("Returning an existing transaction");
            return new CreateTransactionResult(existing.get(), true);
        }

        AccountRecord account = accountRepository.findById(request.accountId())
                .orElseThrow(() -> new AccountNotFoundException(request.accountId()));
        validateAccount(account, request.currency());

        Instant now = clock.instant();
        UUID transactionId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        StoredTransaction transaction = new StoredTransaction(
                transactionId,
                request.accountId(),
                request.transactionType(),
                request.monetaryAmount(),
                request.currency(),
                TransactionStatus.SUCCESS,
                now,
                now,
                idempotencyKey,
                fingerprint,
                correlationId
        );
        transactionRepository.insert(transaction);

        TransactionCreatedEvent event = new TransactionCreatedEvent(
                eventId,
                TransactionCreatedEvent.EVENT_TYPE,
                TransactionCreatedEvent.SCHEMA_VERSION,
                transactionId,
                request.accountId(),
                request.transactionType(),
                request.monetaryAmount(),
                request.currency(),
                TransactionStatus.SUCCESS,
                now,
                now,
                correlationId
        );
        outboxRepository.insert(
                eventId,
                transactionId,
                event.eventType(),
                event.schemaVersion(),
                serialize(event),
                now
        );

        LOGGER.atInfo()
                .addKeyValue("transaction_id", transactionId)
                .addKeyValue("event_id", eventId)
                .addKeyValue("account_id", request.accountId())
                .log("Committed transaction and outbox event");
        return new CreateTransactionResult(transaction, false);
    }

    @Transactional(readOnly = true)
    public StoredTransaction findById(UUID transactionId) {
        return transactionRepository.findById(transactionId)
                .orElseThrow(() -> new TransactionNotFoundException(transactionId));
    }

    private void validateAccount(AccountRecord account, String requestedCurrency) {
        if (!"ACTIVE".equals(account.status())) {
            throw new AccountStateConflictException("Account is not active");
        }
        if (!account.currency().equals(requestedCurrency)) {
            throw new AccountStateConflictException("Transaction currency does not match account currency");
        }
    }

    private String serialize(TransactionCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Unable to serialize transaction event", exception);
        }
    }
}
