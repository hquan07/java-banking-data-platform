package io.github.hquan07.banking.analytics.api;

import io.github.hquan07.banking.analytics.support.AnalyticsNotFoundException;
import io.github.hquan07.banking.analytics.support.CorrelationIdFilter;
import io.github.hquan07.banking.analytics.support.InvalidAnalyticsRequestException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.List;

@RestControllerAdvice
public class AnalyticsExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsExceptionHandler.class);

    @ExceptionHandler({
            ConstraintViolationException.class,
            HandlerMethodValidationException.class,
            MissingServletRequestParameterException.class,
            MethodArgumentTypeMismatchException.class
    })
    ResponseEntity<ApiError> invalidRequest(Exception exception, HttpServletRequest request) {
        return error(HttpStatus.BAD_REQUEST, "INVALID_REQUEST",
                "Request is missing a required value or contains an invalid value", request);
    }

    @ExceptionHandler(InvalidAnalyticsRequestException.class)
    ResponseEntity<ApiError> invalidRange(InvalidAnalyticsRequestException exception, HttpServletRequest request) {
        return error(HttpStatus.BAD_REQUEST, "INVALID_ANALYTICS_RANGE", exception.getMessage(), request);
    }

    @ExceptionHandler(AnalyticsNotFoundException.class)
    ResponseEntity<ApiError> notFound(AnalyticsNotFoundException exception, HttpServletRequest request) {
        return error(HttpStatus.NOT_FOUND, "ANALYTICS_NOT_FOUND", exception.getMessage(), request);
    }

    @ExceptionHandler(DataAccessException.class)
    ResponseEntity<ApiError> databaseUnavailable(DataAccessException exception, HttpServletRequest request) {
        LOGGER.error("Warehouse query failed", exception);
        return error(HttpStatus.SERVICE_UNAVAILABLE, "DEPENDENCY_UNAVAILABLE",
                "The analytics warehouse is temporarily unavailable", request);
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ApiError> unexpected(Exception exception, HttpServletRequest request) {
        LOGGER.error("Unexpected analytics request failure", exception);
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
                "The analytics request could not be completed", request);
    }

    private ResponseEntity<ApiError> error(
            HttpStatus status,
            String code,
            String message,
            HttpServletRequest request
    ) {
        Object correlationId = request.getAttribute(CorrelationIdFilter.REQUEST_ATTRIBUTE);
        return ResponseEntity.status(status).body(new ApiError(
                code, message, String.valueOf(correlationId), List.of()
        ));
    }
}
