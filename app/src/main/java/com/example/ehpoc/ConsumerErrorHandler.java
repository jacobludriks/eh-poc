package com.example.ehpoc;

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * The container's error handler (Spring Boot wires a {@code CommonErrorHandler} bean into the listener container).
 * Standard behaviour, plus recording the class of errors raised outside record processing (poll, authentication,
 * authorisation, group coordination) so /api/diagnostics can show why the consumer is not receiving.
 */
@Component
public class ConsumerErrorHandler extends DefaultErrorHandler {

    private final ConsumerStatus status;

    public ConsumerErrorHandler(ConsumerStatus status) {
        this.status = status;
    }

    @Override
    public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer,
            MessageListenerContainer container, boolean batchListener) {
        status.failed(thrownException);
        super.handleOtherException(thrownException, consumer, container, batchListener);
    }
}
