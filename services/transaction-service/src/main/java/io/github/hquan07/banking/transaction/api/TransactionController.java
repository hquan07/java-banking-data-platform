package io.github.hquan07.banking.transaction.api;

import io.github.hquan07.banking.transaction.application.CreateTransactionResult;
import io.github.hquan07.banking.transaction.application.TransactionApplicationService;
import io.github.hquan07.banking.transaction.persistence.StoredTransaction;
import io.github.hquan07.banking.transaction.support.CorrelationIdFilter;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.UUID;

@Validated
@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {

    private final TransactionApplicationService transactionService;

    public TransactionController(TransactionApplicationService transactionService) {
        this.transactionService = transactionService;
    }

    @PostMapping
    public ResponseEntity<TransactionResponse> create(
            @Valid @RequestBody TransactionRequest request,
            @RequestHeader("Idempotency-Key")
            @Pattern(
                    regexp = "[A-Za-z0-9._:-]{1,128}",
                    message = "must contain 1 to 128 URL-safe characters"
            )
            String idempotencyKey,
            @RequestAttribute(CorrelationIdFilter.REQUEST_ATTRIBUTE) String correlationId
    ) {
        CreateTransactionResult result = transactionService.create(request, idempotencyKey, correlationId);
        TransactionResponse response = response(result.transaction(), correlationId, result.idempotentReplay());
        if (result.idempotentReplay()) {
            return ResponseEntity.ok(response);
        }

        URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{transactionId}")
                .buildAndExpand(result.transaction().transactionId())
                .toUri();
        return ResponseEntity.created(location).body(response);
    }

    @GetMapping("/{transactionId}")
    public TransactionResponse findById(
            @PathVariable("transactionId") UUID transactionId,
            @RequestAttribute(CorrelationIdFilter.REQUEST_ATTRIBUTE) String correlationId
    ) {
        return response(transactionService.findById(transactionId), correlationId, false);
    }

    private TransactionResponse response(
            StoredTransaction transaction,
            String correlationId,
            boolean idempotentReplay
    ) {
        return new TransactionResponse(
                transaction.transactionId(),
                transaction.accountId(),
                transaction.transactionType(),
                transaction.amount().toPlainString(),
                transaction.currency(),
                transaction.status(),
                transaction.occurredAt(),
                transaction.createdAt(),
                correlationId,
                idempotentReplay
        );
    }
}
