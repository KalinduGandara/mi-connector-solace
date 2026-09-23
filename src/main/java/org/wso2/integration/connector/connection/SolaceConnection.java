/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.integration.connector.connection;

import com.solace.messaging.trace.propagation.SolaceJCSMPTextMapSetter;
import com.solacesystems.jcsmp.Browser;
import com.solacesystems.jcsmp.BrowserProperties;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.CacheLiveDataAction;
import com.solacesystems.jcsmp.CacheRequestResult;
import com.solacesystems.jcsmp.CacheSession;
import com.solacesystems.jcsmp.CacheSessionProperties;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPRequestTimeoutException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.ProducerFlowProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Requestor;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLContentMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.TransactedSession;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.aspects.flow.statistics.collectors.RuntimeStatisticCollector;
import org.apache.synapse.aspects.flow.statistics.tracing.opentelemetry.management.handling.span.SpanHandler;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.Connection;
import org.wso2.integration.connector.core.connection.ConnectionConfig;
import org.wso2.integration.connector.models.PublishResult;
import org.wso2.integration.connector.models.SolaceMessageProperties;

/**
 * Manages a connection to the Solace event broker.
 * Wraps a JCSMPSession and XMLMessageProducer.
 */
public class SolaceConnection implements Connection {

    private static final Log log = LogFactory.getLog(SolaceConnection.class);

    /**
     * Writes W3C trace context into a JCSMP message.
     */
    private static final SolaceJCSMPTextMapSetter TRACE_CONTEXT_SETTER = new SolaceJCSMPTextMapSetter();

    private JCSMPSession session;
    private XMLMessageProducer producer;
    private XMLMessageConsumer consumer;
    private final Object consumerLock = new Object();
    private final String connectionId;
    private TransactedSession txSession;
    private XMLMessageProducer txProducer;
    /**
     * Transacted FlowReceivers, one per queue name, lazily created the first time the
     * caller polls a given queue inside the current transaction. The selector each
     * receiver was created with is recorded so subsequent polls of the same queue can
     * be rejected if they request a different filter — JCSMP binds selectors at flow
     * creation, so a mismatch on reuse would silently apply the wrong filter.
     * Lifecycle is bound to {@link #txSession}: all receivers are closed in
     * {@link #closeTransactedSessionInternal} on commit/rollback.
     */
    private final Map<String, TxFlowEntry> txFlowReceivers = new ConcurrentHashMap<>();
    private final Object txLock = new Object();

    /**
     * Tracks in-flight guaranteed publishes when the caller asked to block for the broker ACK.
     * Keyed by correlation key. The {@link PublishEventHandler} completes the future on ack or
     * error; the calling thread then unblocks from {@link #publish}.
     */
    private final ConcurrentMap<String, CompletableFuture<Void>> pendingAcks = new ConcurrentHashMap<>();

    /**
     * Creates a new SolaceConnection with a pre-built JCSMPSession.
     *
     * @param session      the JCSMP session
     * @param connectionId a unique identifier for this connection
     * @throws JCSMPException if creating the producer fails
     */
    public SolaceConnection(JCSMPSession session, String connectionId) throws JCSMPException {
        this.session = session;
        this.connectionId = connectionId;
        this.producer = session.getMessageProducer(new PublishEventHandler());
        // The session consumer is created lazily (see ensureConsumer): 
        // Only operations that receive on the session ( requestCachedMessages and DIRECT sendRequest) create consumer.
        log.info("Solace connection created: " + connectionId);
    }

    /**
     * Publishes a message to the specified destination. When {@code waitForAck} is true
     * and the delivery mode is guaranteed (PERSISTENT / NON_PERSISTENT), blocks until
     * the broker ACKs the message or the ACK times out — so the caller knows the broker
     * accepted the message before the mediation continues. DIRECT deliveries never
     * produce ACKs, so {@code waitForAck} has no effect for them.
     *
     * @param destinationType  TOPIC or QUEUE
     * @param destinationName  the name of the topic or queue
     * @param payload          the message payload as a string
     * @param deliveryMode     DIRECT, PERSISTENT, or NON_PERSISTENT
     * @param messageType      message type: TEXT, BYTES, or XML (default: TEXT; JSON travels as TEXT)
     * @param properties       additional message properties (may be null)
     * @param waitForAck       if true and delivery is guaranteed, block for broker ACK
     * @param ackTimeoutMillis maximum time to wait for the broker ACK (ignored when waitForAck=false)
     * @param synCtx           mediation context of the caller, used to propagate the trace context
     * @throws JCSMPException if publishing fails, the broker NACKs, or the ACK wait times out
     */
    public PublishResult publish(String destinationType, String destinationName, String payload,
                                 String deliveryMode, String messageType, SolaceMessageProperties properties,
                                 boolean waitForAck, long ackTimeoutMillis, MessageContext synCtx)
            throws JCSMPException {
        return publish(destinationType, destinationName, payload, deliveryMode, messageType, properties,
                waitForAck, ackTimeoutMillis, null, synCtx);
    }

    public PublishResult publish(String destinationType, String destinationName, String payload,
                                 String deliveryMode, String messageType, SolaceMessageProperties properties,
                                 boolean waitForAck, long ackTimeoutMillis, String httpContentType,
                                 MessageContext synCtx)
            throws JCSMPException {

        // Check whether multiple calls cause error or handled internally
        Destination destination = resolveDestination(destinationType, destinationName);

        BytesXMLMessage message = createMessage(messageType, payload, synCtx);
        if (httpContentType != null && !httpContentType.isEmpty()) {
            message.setHTTPContentType(httpContentType);
        }

        // Set delivery mode
        if (deliveryMode != null) {
            switch (deliveryMode.toUpperCase()) {
                case SolaceConstants.DELIVERY_MODE_PERSISTENT:
                    message.setDeliveryMode(DeliveryMode.PERSISTENT);
                    break;
                case SolaceConstants.DELIVERY_MODE_NON_PERSISTENT:
                    message.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    break;
                case SolaceConstants.DELIVERY_MODE_DIRECT:
                default:
                    message.setDeliveryMode(DeliveryMode.DIRECT);
                    break;
            }
        }

        applyMessageProperties(message, properties);

        // Set correlation key for guaranteed delivery tracking
        boolean guaranteed = message.getDeliveryMode() != DeliveryMode.DIRECT;
        String correlationKey = null;
        CompletableFuture<Void> ackFuture = null;
        if (guaranteed) {
            correlationKey = destinationName + "-" + UUID.randomUUID();
            message.setCorrelationKey(correlationKey);
            if (waitForAck) {
                ackFuture = new CompletableFuture<>();
                pendingAcks.put(correlationKey, ackFuture);
            }
        }

        try {
            producer.send(message, destination);
        } catch (JCSMPException e) {
            if (correlationKey != null) {
                pendingAcks.remove(correlationKey);
            }
            throw e;
        }

        // Transport succeeded. Beyond this point we only distinguish ACK outcomes,
        // returning them as PublishResult so the caller can decide how to react.
        if (ackFuture != null) {
            try {
                ackFuture.get(ackTimeoutMillis, TimeUnit.MILLISECONDS);
                if (log.isDebugEnabled()) {
                    log.debug("Message published to " + destinationType + " '" + destinationName
                            + "' (ACK received, correlationKey=" + correlationKey + ")");
                }
                return new PublishResult(SolaceConstants.ACK_STATUS_ACK, true, correlationKey, null);
            } catch (TimeoutException e) {
                String msg = "Publish ACK timeout after " + ackTimeoutMillis + "ms";
                return new PublishResult(SolaceConstants.ACK_STATUS_TIMEOUT, false, correlationKey, msg);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                String msg = cause != null ? cause.getMessage() : "Broker NACK";
                return new PublishResult(SolaceConstants.ACK_STATUS_NACK, false, correlationKey, msg);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new PublishResult(SolaceConstants.ACK_STATUS_NACK, false, correlationKey,
                        "Interrupted while waiting for publish ACK");
            } finally {
                // Single cleanup point for the tracking entry, whatever the outcome. On ACK/NACK
                // the event handler already removed it before completing the future; on
                // timeout/interrupt no callback fired, so this is where it's removed. remove()
                // on an already-absent key is a harmless no-op.
                pendingAcks.remove(correlationKey);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Message published to " + destinationType + " '" + destinationName + "'");
        }
        String status = guaranteed ? SolaceConstants.ACK_STATUS_NOT_REQUESTED
                : SolaceConstants.ACK_STATUS_NOT_APPLICABLE;
        return new PublishResult(status, false, correlationKey, null);
    }

    /**
     * Sends a reply to the original inbound request message using JCSMP's native sendReply API.
     * The reply-to destination is derived from the original request. Correlation ID and
     * application message ID are echoed from the request when the caller has not supplied
     * explicit values via {@code properties}.
     *
     * @param inboundMessage the original request message received by the inbound endpoint
     * @param payload        the reply payload
     * @param deliveryMode   DIRECT, PERSISTENT, or NON_PERSISTENT (default: DIRECT)
     * @param messageType    message type: TEXT, BYTES, or XML (default: TEXT; JSON travels as TEXT)
     * @param properties     additional reply message properties (may be null)
     * @param synCtx         mediation context of the caller, used to propagate the trace context
     * @throws JCSMPException if publishing fails
     */
    public void sendReply(BytesXMLMessage inboundMessage, String payload, String deliveryMode,
                          String messageType, SolaceMessageProperties properties,
                          MessageContext synCtx) throws JCSMPException {
        sendReply(inboundMessage, payload, deliveryMode, messageType, properties, null, synCtx);
    }

    public void sendReply(BytesXMLMessage inboundMessage, String payload, String deliveryMode,
                          String messageType, SolaceMessageProperties properties,
                          String httpContentType, MessageContext synCtx) throws JCSMPException {

        BytesXMLMessage replyMessage = createMessage(messageType, payload, synCtx);
        if (httpContentType != null && !httpContentType.isEmpty()) {
            replyMessage.setHTTPContentType(httpContentType);
        }

        if (deliveryMode != null) {
            switch (deliveryMode.toUpperCase()) {
                case SolaceConstants.DELIVERY_MODE_PERSISTENT:
                    replyMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
                    break;
                case SolaceConstants.DELIVERY_MODE_NON_PERSISTENT:
                    replyMessage.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    break;
                case SolaceConstants.DELIVERY_MODE_DIRECT:
                default:
                    replyMessage.setDeliveryMode(DeliveryMode.DIRECT);
                    break;
            }
        } else {
            replyMessage.setDeliveryMode(DeliveryMode.DIRECT);
        }

        // Echo correlationId and applicationMessageId from the request if the caller
        // didn't explicitly supply them — matches the JCSMP request-reply convention.
        if (properties == null || properties.getCorrelationId() == null) {
            if (inboundMessage.getCorrelationId() != null) {
                replyMessage.setCorrelationId(inboundMessage.getCorrelationId());
            }
        }
        if (properties == null || properties.getApplicationMessageId() == null) {
            if (inboundMessage.getApplicationMessageId() != null) {
                replyMessage.setApplicationMessageId(inboundMessage.getApplicationMessageId());
            }
        }

        applyMessageProperties(replyMessage, properties);

        // Set correlation key for guaranteed delivery tracking
        if (replyMessage.getDeliveryMode() != DeliveryMode.DIRECT) {
            String correlationKey = "reply-" + UUID.randomUUID();
            replyMessage.setCorrelationKey(correlationKey);
        }

        producer.sendReply(inboundMessage, replyMessage);
        if (log.isDebugEnabled()) {
            log.debug("Reply sent to " + inboundMessage.getReplyTo());
        }
    }

    /**
     * Performs a synchronous request-reply. For DIRECT delivery, uses the JCSMP
     * {@link Requestor} API. For guaranteed (PERSISTENT/NON_PERSISTENT) delivery,
     * provisions a temporary queue, sets it as the reply-to destination, and waits
     * for a reply via a {@link FlowReceiver} — this is the only JCSMP-supported
     * pattern for guaranteed request-reply.
     *
     * @param destinationType TOPIC or QUEUE
     * @param destinationName destination name
     * @param payload         request payload
     * @param deliveryMode    DIRECT, PERSISTENT, or NON_PERSISTENT
     * @param messageType     message type: TEXT, BYTES, or XML (JSON travels as TEXT)
     * @param properties      optional request message properties (may be null)
     * @param timeoutMillis   maximum time to wait for a reply
     * @param synCtx          mediation context of the caller, used to propagate the trace context
     * @return the reply message
     * @throws JCSMPRequestTimeoutException if no reply arrives within the timeout
     * @throws JCSMPException               if the request cannot be sent or flow setup fails
     */
    public BytesXMLMessage sendRequest(String destinationType, String destinationName, String payload,
                                       String deliveryMode, String messageType,
                                       SolaceMessageProperties properties, long timeoutMillis, MessageContext synCtx)
            throws JCSMPException {
        return sendRequest(destinationType, destinationName, payload, deliveryMode, messageType,
                properties, timeoutMillis, null, synCtx);
    }

    public BytesXMLMessage sendRequest(String destinationType, String destinationName, String payload,
                                       String deliveryMode, String messageType,
                                       SolaceMessageProperties properties, long timeoutMillis,
                                       String httpContentType, MessageContext synCtx)
            throws JCSMPException {

        Destination destination = resolveDestination(destinationType, destinationName);
        DeliveryMode resolvedMode = resolveDeliveryMode(deliveryMode);

        BytesXMLMessage requestMessage = createMessage(messageType, payload, synCtx);
        if (httpContentType != null && !httpContentType.isEmpty()) {
            requestMessage.setHTTPContentType(httpContentType);
        }
        requestMessage.setDeliveryMode(resolvedMode);
        applyMessageProperties(requestMessage, properties);

        if (resolvedMode == DeliveryMode.DIRECT) {
            // DIRECT: JCSMP Requestor handles reply-to temp topic + blocking receive.
            // The Requestor needs the session consumer started to receive the reply
            // This matches replies by correlation ID
            ensureConsumer();
            Requestor requestor = session.createRequestor();
            return requestor.request(requestMessage, timeoutMillis, destination);
        }

        // Guaranteed request-reply: temp queue + FlowReceiver
        return sendGuaranteedRequest(requestMessage, destination, destinationName, timeoutMillis);
    }

    /**
     * Executes a guaranteed request-reply using a temporary queue.
     * The temp queue is provisioned, attached as the reply-to destination, and drained
     * by a short-lived FlowReceiver. Resources are released in the finally block to avoid
     * leaking flows or temp endpoints on errors.
     *
     * <p>A correlation ID is set on the request (generated if the caller didn't supply one)
     * and verified against the reply. If the reply's correlation ID doesn't match — which
     * shouldn't happen on a fresh temp queue but is defensive — a warning is logged.</p>
     */
    private BytesXMLMessage sendGuaranteedRequest(BytesXMLMessage requestMessage, Destination destination,
                                                  String destinationName, long timeoutMillis)
            throws JCSMPException {

        Queue tempQueue = session.createTemporaryQueue();
        requestMessage.setReplyTo(tempQueue);

        // Ensure the request carries a correlation ID we can verify against the reply.
        // If the caller already set one via properties, use it; otherwise generate one.
        String expectedCorrelationId = requestMessage.getCorrelationId();
        if (expectedCorrelationId == null) {
            expectedCorrelationId = "req-" + UUID.randomUUID();
            requestMessage.setCorrelationId(expectedCorrelationId);
        }

        // Set correlation key for guaranteed publish-ACK tracking
        String correlationKey = destinationName + "-req-" + UUID.randomUUID();
        requestMessage.setCorrelationKey(correlationKey);

        ConsumerFlowProperties flowProps = new ConsumerFlowProperties();
        flowProps.setEndpoint(tempQueue);
        flowProps.setStartState(true);

        FlowReceiver flowReceiver = null;
        try {
            flowReceiver = session.createFlow(null, flowProps);

            producer.send(requestMessage, destination);

            // Block until a reply arrives or the timeout expires
            BytesXMLMessage reply = flowReceiver.receive((int) timeoutMillis);
            if (reply == null) {
                throw new JCSMPRequestTimeoutException("No reply received within " + timeoutMillis + "ms");
            }

            // Verify the reply corresponds to our request. A mismatch is unlikely on a fresh
            // temp queue, but defensive logging makes cross-wiring easier to diagnose.
            String replyCorrelationId = reply.getCorrelationId();
            if (replyCorrelationId != null && !expectedCorrelationId.equals(replyCorrelationId)) {
                log.warn("Guaranteed request-reply correlation mismatch: expected='"
                        + expectedCorrelationId + "', received='" + replyCorrelationId
                        + "'. Returning the message regardless.");
            }

            // Acknowledge so the broker can remove it from the temp queue
            reply.ackMessage();
            return reply;
        } finally {
            if (flowReceiver != null) {
                flowReceiver.close();
            }
            // Temp queue is auto-cleaned when the session closes; no explicit deprovision needed.
        }
    }

    /**
     * Resolves a delivery mode string into the JCSMP enum. Null or unknown falls back to DIRECT.
     */
    private DeliveryMode resolveDeliveryMode(String deliveryMode) {
        if (deliveryMode == null) {
            return DeliveryMode.DIRECT;
        }
        switch (deliveryMode.toUpperCase()) {
            case SolaceConstants.DELIVERY_MODE_PERSISTENT:
                return DeliveryMode.PERSISTENT;
            case SolaceConstants.DELIVERY_MODE_NON_PERSISTENT:
                return DeliveryMode.NON_PERSISTENT;
            case SolaceConstants.DELIVERY_MODE_DIRECT:
            default:
                return DeliveryMode.DIRECT;
        }
    }

    /**
     * Creates a message of the specified type with the given payload. Supports TEXT, BYTES, and XML.
     * JSON payloads are carried as TEXT on the wire (Solace has no JSON message type); callers are
     * expected to pass messageType=TEXT for JSON content.
     *
     * @param messageType message type: TEXT, BYTES, or XML (default: TEXT)
     * @param payload     the message payload
     * @param synCtx      mediation context of the caller, used to propagate the trace context
     * @return the created message, carrying the trace context when tracing is enabled
     */
    private BytesXMLMessage createMessage(String messageType, String payload, MessageContext synCtx) {
        if (messageType == null) {
            messageType = SolaceConstants.MESSAGE_TYPE_TEXT;
        }
        BytesXMLMessage message;
        switch (messageType.toUpperCase()) {
            case SolaceConstants.MESSAGE_TYPE_BYTES:
                BytesMessage bytesMsg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
                bytesMsg.setData(payload != null ? payload.getBytes() : new byte[0]);
                message = bytesMsg;
                break;
            case SolaceConstants.MESSAGE_TYPE_XML:
                XMLContentMessage xmlMsg = JCSMPFactory.onlyInstance().createMessage(XMLContentMessage.class);
                if (payload != null) {
                    xmlMsg.setXMLContent(payload);
                }
                message = xmlMsg;
                break;
            case SolaceConstants.MESSAGE_TYPE_TEXT:
            default:
                TextMessage textMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                textMsg.setText(payload);
                message = textMsg;
        }
        if (RuntimeStatisticCollector.isOpenTelemetryEnabled()) {
            try {
                SpanHandler.injectTraceContext(synCtx, message, TRACE_CONTEXT_SETTER);
            } catch (NoSuchMethodError e) {
                log.warn("You are running solace connector on old wso2mi version. " +
                        "Distributed tracing will not working correctly in this version. " +
                        "Please upgrade to wso2mi 4.6.0.12 or above.");
            }
        }
        return message;
    }

    /**
     * Applies optional SolaceMessageProperties to a message.
     */
    private void applyMessageProperties(BytesXMLMessage message, SolaceMessageProperties properties) {
        if (properties == null) {
            return;
        }
        if (properties.getCorrelationId() != null) {
            message.setCorrelationId(properties.getCorrelationId());
        }
        if (properties.getPriority() != null) {
            message.setPriority(properties.getPriority());
        }
        if (properties.getTimeToLive() != null) {
            message.setTimeToLive(properties.getTimeToLive());
        }
        if (properties.getApplicationMessageId() != null) {
            message.setApplicationMessageId(properties.getApplicationMessageId());
        }
        if (properties.getApplicationMessageType() != null) {
            message.setApplicationMessageType(properties.getApplicationMessageType());
        }
        if (properties.getSenderId() != null) {
            message.setSenderId(properties.getSenderId());
        }
        if (properties.getDmqEligible() != null) {
            message.setDMQEligible(properties.getDmqEligible());
        }
        if (properties.getElidingEligible() != null) {
            message.setElidingEligible(properties.getElidingEligible());
        }
        if (properties.getExpiration() != null) {
            message.setExpiration(properties.getExpiration());
        }
        if (properties.getReplyToDestinationType() != null && properties.getReplyToDestinationName() != null) {
            Destination replyTo = resolveDestination(properties.getReplyToDestinationType(),
                    properties.getReplyToDestinationName());
            message.setReplyTo(replyTo);
        }
        // Set user properties (custom key-value pairs)
        if (properties.getUserProperties() != null && !properties.getUserProperties().isEmpty()) {
            try {
                SDTMap userPropsMap = JCSMPFactory.onlyInstance().createMap();
                for (Map.Entry<String, String> entry : properties.getUserProperties().entrySet()) {
                    userPropsMap.putString(entry.getKey(), entry.getValue());
                }
                message.setProperties(userPropsMap);
            } catch (SDTException e) {
                log.error("Error setting user properties on message", e);
            }
        }
    }

    /**
     * Resolves a destination (topic or queue) from type and name.
     */
    private Destination resolveDestination(String type, String name) {
        if (SolaceConstants.DESTINATION_TYPE_QUEUE.equalsIgnoreCase(type)) {
            return JCSMPFactory.onlyInstance().createQueue(name);
        }
        return JCSMPFactory.onlyInstance().createTopic(name);
    }

    /**
     * Gets the underlying JCSMP session.
     *
     * @return the session
     */
    public JCSMPSession getSession() {
        return session;
    }

    /**
     * Gets the connection identifier.
     *
     * @return the connection ID
     */
    public String getConnectionId() {
        return connectionId;
    }

    /**
     * Checks if the connection is active.
     *
     * @return true if the session is not closed
     */
    public boolean isConnected() {
        return session != null && !session.isClosed();
    }

    public TransactedSession getOrCreateTransactedSession() throws JCSMPException {
        synchronized (txLock) {
            if (txSession == null) {
                log.debug("Creating new transacted session on connection: " + connectionId);
                // Build into a local first so a producer-create failure can't leave the
                // connection with txSession set but txProducer null — that state would
                // poison the connection for every subsequent borrower from the pool.
                TransactedSession newSession = session.createTransactedSession();
                try {
                    ProducerFlowProperties producerProps = new ProducerFlowProperties();
                    txProducer = newSession.createProducer(producerProps, new PublishEventHandler());
                    txSession = newSession;
                    log.debug("Transacted session created on connection: " + connectionId);
                } catch (JCSMPException e) {
                    log.error("Failed to create transacted producer on connection: " + connectionId
                            + " — closing transacted session: " + e.getMessage(), e);
                    try {
                        // Close the half-built transacted session so it isn't leaked; the
                        // original producer-create failure is rethrown below.
                        newSession.close();
                    } catch (Exception ignored) {
                        if (log.isDebugEnabled()) {
                            log.debug("Error closing half-built transacted session during cleanup on"
                                    + " connection: " + connectionId + " — " + ignored.getMessage());
                        }
                    }
                    throw e;
                }
            } else {
                log.info("Reusing existing transacted session on connection: " + connectionId);
            }
            return txSession;
        }
    }

    public void commitTransaction() throws JCSMPException {
        synchronized (txLock) {
            if (txSession == null) {
                log.error("commitTransaction called but no active transacted session on connection: "
                        + connectionId);
                throw new JCSMPException("No active transaction on connection: " + connectionId);
            }
            log.debug("Committing transacted session on connection: " + connectionId);
            try {
                txSession.commit();
                log.info("Transaction committed on connection: " + connectionId);
            } catch (JCSMPException e) {
                log.error("Transacted session commit failed on connection: " + connectionId
                        + ": " + e.getMessage(), e);
                throw e;
            } finally {
                closeTransactedSessionInternal();
                log.debug("Transacted session closed on connection: " + connectionId + " (post-commit)");
            }
        }
    }

    public void rollbackTransaction() throws JCSMPException {
        synchronized (txLock) {
            if (txSession == null) {
                log.error("rollbackTransaction called but no active transacted session on connection: "
                        + connectionId);
                throw new JCSMPException("No active transaction on connection: " + connectionId);
            }
            log.debug("Rolling back transacted session on connection: " + connectionId);
            try {
                txSession.rollback();
                log.warn("Transaction rolled back on connection: " + connectionId);
            } catch (JCSMPException e) {
                log.error("Transacted session rollback failed on connection: " + connectionId
                        + ": " + e.getMessage(), e);
                throw e;
            } finally {
                closeTransactedSessionInternal();
                log.debug("Transacted session closed on connection: " + connectionId + " (post-rollback)");
            }
        }
    }

    public boolean hasActiveTransaction() {
        synchronized (txLock) {
            return txSession != null;
        }
    }

    private void closeTransactedSessionInternal() {
        // Close any transacted FlowReceivers first — they live on the txSession and would
        // otherwise be orphaned by closing the session out from under them.
        if (!txFlowReceivers.isEmpty()) {
            for (Map.Entry<String, TxFlowEntry> entry : txFlowReceivers.entrySet()) {
                try {
                    entry.getValue().receiver.close();
                } catch (Exception e) {
                    log.warn("Error closing transacted flow receiver for queue '"
                            + entry.getKey() + "' on connection " + connectionId + ": "
                            + e.getMessage());
                }
            }
            txFlowReceivers.clear();
        }
        if (txProducer != null) {
            txProducer.close();
            txProducer = null;
        }
        if (txSession != null) {
            txSession.close();
            txSession = null;
        }
    }

    public void closeTransactedSession() {
        synchronized (txLock) {
            closeTransactedSessionInternal();
        }
    }

    public PublishResult publishTransacted(String destinationType, String destinationName,
                                       String payload, String deliveryMode, String messageType,
                                       SolaceMessageProperties properties, String httpContentType,
                                       MessageContext synCtx)
        throws JCSMPException {
        synchronized (txLock) {
            if (txProducer == null) {
                log.error("publishTransacted called but no transacted session active on connection: "
                        + connectionId);
                throw new JCSMPException(
                    "publishTransacted called but no transacted session active on connection: "
                    + connectionId);
            }
            Destination destination = resolveDestination(destinationType, destinationName);
            BytesXMLMessage msg = createMessage(messageType, payload, synCtx);
            if (httpContentType != null && !httpContentType.isEmpty()) {
                msg.setHTTPContentType(httpContentType);
            }
            msg.setDeliveryMode(resolveDeliveryMode(deliveryMode));
            applyMessageProperties(msg, properties);
            // Stable correlation key per transacted publish — surfaced in the PublishResult and
            // visible in broker-side message-spool browsing for "where did this message go" diagnostics.
            String correlationKey = "tx-" + connectionId + "-" + UUID.randomUUID();
            msg.setCorrelationKey(correlationKey);
            log.debug("publishTransacted: sending to " + destinationType + " '" + destinationName
                    + "' on connectionId=" + connectionId + " (correlationKey=" + correlationKey + ")");
            try {
                txProducer.send(msg, destination);
            } catch (JCSMPException e) {
                log.error("publishTransacted: send failed on connectionId=" + connectionId
                        + " (correlationKey=" + correlationKey + "): " + e.getMessage(), e);
                throw e;
            }
            log.debug("publishTransacted: send returned (pending commit) on connectionId=" + connectionId
                    + " (correlationKey=" + correlationKey + ")");
            return new PublishResult(SolaceConstants.ACK_STATUS_TX_PENDING, false, correlationKey, null);
        }
    }

    /**
     * Synchronously polls one message from a queue inside the active transaction. The
     * message is held by the broker until the surrounding transaction commits (then acked
     * and removed) or rolls back (then redelivered) — the caller does not (and cannot)
     * settle it manually.
     *
     * <p>Provisions a transacted FlowReceiver on the txSession the first time a given
     * queue is polled in this transaction; subsequent polls of the same queue reuse the
     * same receiver. All receivers are closed by {@link #closeTransactedSessionInternal}
     * on commit/rollback.</p>
     *
     * @param queueName     the durable queue to poll
     * @param timeoutMillis maximum time to block waiting for a message
     * @param selector      optional JMS-style selector expression (may be null);
     *                      ignored on subsequent calls if the receiver already exists
     * @return the received message, or null on timeout
     * @throws JCSMPException if no transacted session is active, or flow setup / receive fails
     */
    public BytesXMLMessage pollTransacted(String queueName, long timeoutMillis, String selector)
            throws JCSMPException {
        synchronized (txLock) {
            if (txSession == null) {
                log.error("pollTransacted called but no transacted session active on connection: "
                        + connectionId);
                throw new JCSMPException(
                        "pollTransacted called but no transacted session active on connection: "
                        + connectionId);
            }

            String normalizedSelector = (selector == null || selector.isEmpty()) ? null : selector;
            TxFlowEntry entry = txFlowReceivers.get(queueName);
            if (entry == null) {
                Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
                ConsumerFlowProperties flowProps = new ConsumerFlowProperties();
                flowProps.setEndpoint(queue);
                flowProps.setStartState(true);
                if (normalizedSelector != null) {
                    flowProps.setSelector(normalizedSelector);
                }
                EndpointProperties endpointProps = new EndpointProperties();
                endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

                log.debug("pollTransacted: creating transacted flow for queue '" + queueName
                        + "' on connectionId=" + connectionId
                        + (normalizedSelector != null ? " (selector='" + normalizedSelector + "')" : ""));
                // Settlement on a transacted flow is implicit (commit/rollback), so we do NOT
                // set CLIENT_ACK mode here — that's only for the non-transacted poll path.
                FlowReceiver receiver = txSession.createFlow(null, flowProps, endpointProps);
                entry = new TxFlowEntry(receiver, normalizedSelector);
                txFlowReceivers.put(queueName, entry);
            } else if (!java.util.Objects.equals(entry.selector, normalizedSelector)) {
                // JCSMP binds the selector at flow-creation time. Silently ignoring the new
                // selector and reusing a flow with a different filter would be a correctness
                // bug — reject the call so the caller can split this into a separate
                // transaction (one selector per queue per tx).
                throw new JCSMPException(
                        "pollTransacted: selector mismatch on queue '" + queueName + "' (existing='"
                        + entry.selector + "', requested='" + normalizedSelector + "'). A queue's"
                        + " transacted flow is created once per transaction with a fixed selector;"
                        + " use only one selector per queue within a single transaction.");
            }

            return entry.receiver.receive((int) timeoutMillis);
        }
    }

    /**
     * Bookkeeping for a transacted FlowReceiver — pairs the receiver with the selector
     * it was created with so we can reject mismatched reuses inside the same transaction.
     */
    private static final class TxFlowEntry {
        final FlowReceiver receiver;
        final String selector;
        TxFlowEntry(FlowReceiver receiver, String selector) {
            this.receiver = receiver;
            this.selector = selector;
        }
    }

    /**
     * Synchronously polls one message from a queue and acknowledges it on receipt.
     * Provisions a short-lived FlowReceiver on the queue, waits up to {@code timeoutMillis}
     * for a message, acks it (so the broker removes it from the queue), and tears the flow
     * down before returning.
     *
     * <p><b>Settlement is atomic with consumption</b> — manual ack/nack of polled messages
     * is not supported because the flow closes before mediation continues. For atomic
     * consume-then-publish or rollback-on-failure semantics, wrap publish operations in
     * {@code beginTransaction} / {@code commit} (consumer-side transactional support).</p>
     *
     * @param queueName     the durable queue to poll
     * @param timeoutMillis maximum time to block waiting for a message
     * @param selector      optional JMS-style selector expression (may be null)
     * @return the received message, or null on timeout
     * @throws JCSMPException if flow setup or receive fails
     */
    public BytesXMLMessage pollMessage(String queueName, long timeoutMillis, String selector)
            throws JCSMPException {
        Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);

        ConsumerFlowProperties flowProps = new ConsumerFlowProperties();
        flowProps.setEndpoint(queue);
        flowProps.setStartState(true);
        flowProps.setAckMode(com.solacesystems.jcsmp.JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
        if (selector != null && !selector.isEmpty()) {
            flowProps.setSelector(selector);
        }

        EndpointProperties endpointProps = new EndpointProperties();
        endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

        FlowReceiver flowReceiver = null;
        try {
            flowReceiver = session.createFlow(null, flowProps, endpointProps);
            BytesXMLMessage message = flowReceiver.receive((int) timeoutMillis);
            if (message != null) {
                message.ackMessage();
            }
            return message;
        } finally {
            if (flowReceiver != null) {
                flowReceiver.close();
            }
        }
    }

    /**
     * Browses messages on a queue without consuming them. Uses the JCSMP {@link Browser}
     * API. The browser inspects the message-spool replicator without removing messages,
     * so the same messages remain available to live consumers.
     *
     * @param queueName     the queue to browse
     * @param maxMessages   maximum messages to collect; the loop stops earlier if no
     *                      message arrives within {@code timeoutMillis}
     * @param timeoutMillis per-message wait — how long to block between successive
     *                      {@code getNext} calls before declaring the browse done
     * @param selector      optional JMS-style selector expression (may be null)
     * @return a list of browsed messages (never null, may be empty)
     * @throws JCSMPException if browser creation fails
     */
    public List<BytesXMLMessage> browseQueue(String queueName, int maxMessages, long timeoutMillis,
                                             String selector) throws JCSMPException {
        Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);

        BrowserProperties browserProps = new BrowserProperties();
        browserProps.setEndpoint(queue);
        if (selector != null && !selector.isEmpty()) {
            browserProps.setSelector(selector);
        }

        Browser browser = null;
        List<BytesXMLMessage> messages = new ArrayList<>();
        try {
            browser = session.createBrowser(browserProps);
            // getNext blocks up to timeoutMillis. A null return means "nothing more right now",
            // which is the natural stop condition for a one-shot browse — keep simple.
            int browseTimeoutInt = (int) Math.min(timeoutMillis, Integer.MAX_VALUE);
            for (int i = 0; i < maxMessages; i++) {
                BytesXMLMessage msg = browser.getNext(browseTimeoutInt);
                if (msg == null) {
                    break;
                }
                messages.add(msg);
            }
            return messages;
        } finally {
            if (browser != null) {
                browser.close();
            }
        }
    }

    /**
     * DISABLED / NOT YET RELEASED: this method backs the requestCachedMessages operation,
     * which is intentionally hidden from the operation palette (its component is left
     * unregistered in consume/component.xml). It is retained here, compiled but unreachable,
     * and will be released once the operation has been tested end-to-end against a real
     * PubSub+ Cache instance.
     *
     * <p>Issues a Solace Cache request for a topic pattern. Adds a transient subscription on
     * the session, sends a blocking cache request to the named cache instance, then drains
     * any cached messages that arrived on the session's sync consumer. The subscription is
     * removed before returning.
     *
     * <p><b>Prerequisites — this operation only works when the broker side is set up:</b></p>
     * <ul>
     *   <li>A PubSub+ Cache Instance is deployed and running, and joined to the Message VPN.</li>
     *   <li>The cache instance is configured to cache the requested topic pattern (cache
     *       instances cache only topics they have been subscribed to administratively).</li>
     *   <li>The cache instance name passed here matches the broker's cache instance name.</li>
     *   <li>The connecting client has authorization to issue cache requests on the VPN.</li>
     * </ul>
     * <p>If any of these is missing, the broker returns an error and this call throws —
     * the connector cannot detect the misconfiguration ahead of time.</p>
     *
     * @param cacheName        the cache-instance name configured on the broker
     * @param topicPattern     the topic (or wildcard pattern) to request cached messages for
     * @param maxMessages      max messages to request from cache (0 = unlimited per Solace)
     * @param maxAgeSeconds    max age of cached messages in seconds (0 = any age)
     * @param requestTimeoutMs how long the cache request itself may take
     * @param liveDataAction   one of FULFILL, FLOW_THRU, QUEUE — controls how live messages
     *                         on the same topic interleave with the cached delivery. For a
     *                         wildcard {@code topicPattern} (containing '*' or '>'), only
     *                         FLOW_THRU is valid; FULFILL and QUEUE require an exact topic
     * @param drainTimeoutMs   how long to keep draining the consumer for cached messages
     *                         after the cache request returns. Cached messages can arrive
     *                         asynchronously on the session consumer; this is the wait window
     * @return list of received cached messages
     * @throws JCSMPException if cache session creation, subscription, or request fails
     * @throws IllegalArgumentException if a wildcard topic is combined with a live data action
     *                                  other than FLOW_THRU
     */
    public List<BytesXMLMessage> requestCachedMessages(String cacheName, String topicPattern,
                                                       long maxMessages, int maxAgeSeconds,
                                                       long requestTimeoutMs, String liveDataAction,
                                                       long drainTimeoutMs) throws JCSMPException {

        Topic topic = JCSMPFactory.onlyInstance().createTopic(topicPattern);
        CacheLiveDataAction action = resolveLiveDataAction(liveDataAction);

        // Solace constraint: a cache request whose topic contains wildcards ('*' or '>') only
        // supports the FLOW_THRU live data action. FULFILL and QUEUE require an exact topic, and
        // JCSMP rejects the combination deep inside CacheRequestProperties' constructor with
        // "Wildcard topic and live data action <ACTION> not supported". Fail fast here with a
        // clear, actionable message instead of surfacing that raw IllegalArgumentException.
        if ((topicPattern.contains("*") || topicPattern.contains(">"))
                && action != CacheLiveDataAction.FLOW_THRU) {
            throw new IllegalArgumentException(
                    "Cache request for wildcard topic '" + topicPattern + "' requires liveDataAction"
                    + " FLOW_THRU; '" + action + "' is only valid for an exact (non-wildcard) topic."
                    + " Set liveDataAction to FLOW_THRU, or request an exact topic.");
        }

        // CacheSessionProperties has no no-arg constructor; use the 4-arg form
        // (name, maxMsgs, maxAge, timeout). Timeout is per-cache-request in ms.
        CacheSessionProperties cacheProps = new CacheSessionProperties(
                cacheName,
                (int) Math.min(maxMessages, Integer.MAX_VALUE),
                maxAgeSeconds,
                (int) Math.min(requestTimeoutMs, Integer.MAX_VALUE));

        CacheSession cacheSession = session.createCacheSession(cacheProps);
        List<BytesXMLMessage> collected = new ArrayList<>();
        boolean subscribed = false;
        try {
            // Cached messages are delivered to the session consumer
            ensureConsumer();

            // The session consumer is shared and long-lived across pooled reuse. A previous
            // operation (e.g. an earlier cache request that hit its message cap or quiet-period
            // break) may have left messages buffered on it. Clear them first so they can't be
            // mis-attributed to this cache request.
            int stale = drainSessionConsumer();
            if (stale > 0) {
                log.warn("requestCachedMessages: discarded " + stale + " stale message(s) buffered on"
                        + " the session consumer before cache request for topic '" + topicPattern + "'");
            }

            // Subscribe so cached messages route to the session consumer we drain below.
            // On a pooled session the subscription may already be present (subcode 13);
            // tolerate it and reuse it — finally still removes it.
            try {
                session.addSubscription(topic);
            } catch (JCSMPErrorResponseException e) {
                if (e.getSubcodeEx() != JCSMPErrorResponseSubcodeEx.SUBSCRIPTION_ALREADY_PRESENT) {
                    throw e;
                }
                log.debug("Subscription for topic '" + topicPattern + "' already present on the pooled"
                        + " session; reusing it for this cache request.");
            }
            subscribed = true;

            // Blocking cache request — returns when broker finishes streaming cached msgs
            // or the request times out. subscribeFlag MUST be false: we add the subscription
            // ourselves above. Passing true makes JCSMP add the SAME subscription a second time,
            // which the broker rejects with "400: Subscription Already Exists" (subcode 13).
            CacheRequestResult result = cacheSession.sendCacheRequest(
                    Long.valueOf(System.nanoTime()), topic, false, action);
            if (log.isDebugEnabled()) {
                log.debug("Cache request to '" + cacheName + "' for topic '" + topicPattern
                        + "' returned: " + result);
            }

            // Cached messages flow to the session consumer asynchronously. Drain it for the
            // configured window — receive(timeout) returns null when no message is ready.
            long deadline = System.currentTimeMillis() + drainTimeoutMs;
            while (System.currentTimeMillis() < deadline) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    break;
                }
                BytesXMLMessage msg = consumer.receive((int) Math.min(remaining, 500));
                if (msg == null) {
                    // A null after some collected messages typically means cache delivery is done;
                    // break to avoid burning the full drain window when there's nothing left.
                    if (!collected.isEmpty()) {
                        break;
                    }
                    continue;
                }
                collected.add(msg);
                if (maxMessages > 0 && collected.size() >= maxMessages) {
                    break;
                }
            }
            return collected;
        } finally {
            if (subscribed) {
                try {
                    session.removeSubscription(topic);
                } catch (JCSMPException e) {
                    log.warn("Failed to remove transient cache subscription for topic '"
                            + topicPattern + "': " + e.getMessage());
                }
            }
            try {
                // Release the per-request cache session created for this operation.
                cacheSession.close();
            } catch (Exception ignored) {
                if (log.isDebugEnabled()) {
                    log.debug("Error closing cache session during cleanup for topic '" + topicPattern
                            + "' on connection: " + connectionId + " — " + ignored.getMessage());
                }
            }
            // Drain anything still buffered (messages that arrived after we hit the cap or the
            // quiet-period break) so no residue is left on the shared consumer for the next
            // borrower of this pooled connection.
            int leftover = drainSessionConsumer();
            if (leftover > 0 && log.isDebugEnabled()) {
                log.debug("requestCachedMessages: drained " + leftover + " trailing message(s) after"
                        + " cache request for topic '" + topicPattern + "'");
            }
        }
    }

    /**
     * Lazily creates and starts the session's synchronous consumer the first time an
     * operation needs to receive on the session — {@link #requestCachedMessages} (cached
     * messages are delivered here) or the DIRECT branch of {@link #sendRequest} (the
     * {@link Requestor} reads its reply here). Connections that only publish/poll/browse
     * never create one. Idempotent and synchronized so reuse across pooled borrows returns
     * the same started consumer.
     */
    private XMLMessageConsumer ensureConsumer() throws JCSMPException {
        synchronized (consumerLock) {
            if (consumer == null) {
                XMLMessageConsumer newConsumer = null;
                try {
                    // null listener => synchronous consumer (messages pulled via receive()).
                    newConsumer = session.getMessageConsumer((com.solacesystems.jcsmp.XMLMessageListener) null);
                    newConsumer.start();
                    // Publish to the field only once fully started, so a start() failure can't
                    // leave a half-initialized consumer that later calls would blindly reuse.
                    consumer = newConsumer;
                    if (log.isDebugEnabled()) {
                        log.debug("Session consumer started on connection: " + connectionId);
                    }
                } catch (JCSMPException e) {
                    // Close the partially-created consumer so it isn't leaked and the next call
                    // retries from a clean state. Don't log here — the operation layer
                    // (SolaceRequestCachedMessages / SolaceSendRequest) logs and reports this as
                    // a connector fault. Rethrow with context so that single log says where it
                    // failed, instead of duplicate-logging the same error at this hop.
                    if (newConsumer != null) {
                        try {
                            newConsumer.close();
                        } catch (Exception ignored) {
                            if (log.isDebugEnabled()) {
                                log.debug("Error closing partially-created consumer on connection: "
                                        + connectionId + " — " + ignored.getMessage());
                            }
                        }
                    }
                    throw new JCSMPException("Failed to create session consumer on connection "
                            + connectionId, e);
                }
            }
            return consumer;
        }
    }

    /**
     * Drains and discards every message currently buffered on the shared session consumer,
     * returning the count discarded. Non-blocking ({@code receiveNoWait}) — it only clears
     * what is already queued, not what might arrive later. Used to keep cached-message
     * residue from bleeding across pooled reuse of this connection (the session consumer is
     * shared by the cache-request and DIRECT request-reply paths). Best-effort: a receive
     * error just ends the drain. Returns 0 if the consumer was never created.
     */
    private int drainSessionConsumer() {
        synchronized (consumerLock) {
            if (consumer == null) {
                return 0;
            }
            int discarded = 0;
            try {
                while (consumer.receiveNoWait() != null) {
                    discarded++;
                }
            } catch (JCSMPException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Error draining session consumer (stopping drain): " + e.getMessage());
                }
            }
            return discarded;
        }
    }

    private CacheLiveDataAction resolveLiveDataAction(String value) {
        if (value == null) {
            return CacheLiveDataAction.FULFILL;
        }
        switch (value.toUpperCase()) {
            case SolaceConstants.CACHE_LIVE_DATA_FLOW_THRU:
                return CacheLiveDataAction.FLOW_THRU;
            case SolaceConstants.CACHE_LIVE_DATA_QUEUE:
                return CacheLiveDataAction.QUEUE;
            case SolaceConstants.CACHE_LIVE_DATA_FULFILL:
            default:
                return CacheLiveDataAction.FULFILL;
        }
    }

    /**
     * Disconnects the session and releases resources.
     */
    public void disconnect() {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
        if (producer != null) {
            producer.close();
            producer = null;
        }
        // Fail any publishes still blocked waiting for a broker ACK: the session is going away,
        // so no ack/nack callback will ever arrive. Completing the futures exceptionally
        // releases those threads immediately (otherwise they'd wait out the full ack timeout)
        // and clears the tracking map. completeExceptionally on an already-completed future is
        // a no-op, so this is safe against a callback racing in.
        if (!pendingAcks.isEmpty()) {
            JCSMPException closing = new JCSMPException(
                    "Connection " + connectionId + " closed before publish ACK was received");
            for (CompletableFuture<Void> future : pendingAcks.values()) {
                future.completeExceptionally(closing);
            }
            pendingAcks.clear();
        }
        // Always release transacted resources, even if the underlying session was already
        // closed externally (e.g. during a reconnect storm) — prevents producer/session leaks.
        closeTransactedSession();
        if (session != null && !session.isClosed()) {
            session.closeSession();
            log.info("Solace connection closed: " + connectionId);
        }
        session = null;
    }

    @Override
    public void connect(ConnectionConfig connectionConfig) throws ConnectException {
        // Connection is established in the constructor
    }

    @Override
    public void close() throws ConnectException {
        disconnect();
    }

    /**
     * Handler for asynchronous publish events (acks and errors). Non-static so it can
     * signal the {@link #pendingAcks} futures owned by the surrounding connection —
     * required for the synchronous {@code waitForAck} path in {@link #publish}.
     */
    private class PublishEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {

        @Override
        public void responseReceivedEx(Object correlationKey) {
            if (correlationKey instanceof String) {
                CompletableFuture<Void> future = pendingAcks.remove(correlationKey);
                if (future != null) {
                    future.complete(null);
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("Publish ACK received. CorrelationKey: " + correlationKey);
            }
        }

        @Override
        public void handleErrorEx(Object correlationKey, JCSMPException cause, long timestamp) {
            if (correlationKey instanceof String) {
                CompletableFuture<Void> future = pendingAcks.remove(correlationKey);
                if (future != null) {
                    future.completeExceptionally(cause);
                }
            }
            if (cause instanceof JCSMPErrorResponseException) {
                JCSMPErrorResponseException errResp = (JCSMPErrorResponseException) cause;
                log.error("Publish NACK - broker rejected message. CorrelationKey: " + correlationKey
                        + ", subcode: " + errResp.getSubcodeEx()
                        + ", responseCode: " + errResp.getResponseCode()
                        + ", info: " + errResp.getResponsePhrase(), cause);
            } else if (cause instanceof JCSMPTransportException) {
                log.error("Publish error - transport failure. CorrelationKey: " + correlationKey
                        + ", cause: " + cause.getMessage(), cause);
            } else {
                log.error("Publish error. CorrelationKey: " + correlationKey, cause);
            }
        }
    }
}
