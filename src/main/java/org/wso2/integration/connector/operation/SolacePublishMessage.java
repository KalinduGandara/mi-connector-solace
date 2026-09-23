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

package org.wso2.integration.connector.operation;

import com.solacesystems.jcsmp.JCSMPException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONObject;
import org.wso2.integration.connector.connection.SolaceConnection;
import org.wso2.integration.connector.connection.SolaceTransactionRegistry;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.models.PublishResult;
import org.wso2.integration.connector.models.SolaceMessageProperties;
import org.wso2.integration.connector.utils.SolaceUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Publishes a message to Solace (operation class).
 */
public class SolacePublishMessage extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(SolacePublishMessage.class);

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = (String) messageContext.getProperty(SolaceConstants.NAME);
        if (connectionName == null) {
            handleException("Connection name is not set.", messageContext);
            return;
        }
        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        String txId = (String) ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                .getProperty(SolaceConstants.TX_CONNECTION_ID);
        boolean isTransactional = (txId != null);
        if (isTransactional) {
            log.debug("solace.publishMessage: transactional path, txId=" + txId
                    + ", connectionName=" + connectionName);
        }

        SolaceConnection connection = null;
        try {
            if (isTransactional) {
                connection = SolaceTransactionRegistry.get(txId);
                if (connection == null) {
                    log.error("solace.publishMessage: txId=" + txId + " not found in TransactionRegistry");
                    handleException("Transaction " + txId + " not found", messageContext);
                    return;
                }
                log.info("solace.publishMessage: txId=" + txId
                        + " resolved to connectionId=" + connection.getConnectionId());
            } else {
                connection = (SolaceConnection) handler.getConnection(
                        SolaceConstants.CONNECTOR_NAME, connectionName);
                if (connection == null || !connection.isConnected()) {
                    handleException("Solace connection is not available or not connected.", messageContext);
                    return;
                }
            }

            // Extract parameters
            String destinationType = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.DESTINATION_TYPE);
            if (StringUtils.isEmpty(destinationType)) {
                destinationType = SolaceConstants.DESTINATION_TYPE_TOPIC;
            }
            String destinationName = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.DESTINATION_NAME);
            if (StringUtils.isEmpty(destinationName)) {
                handleException("Destination name is required for publishing a message.", messageContext);
                return;
            }

            String deliveryMode = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.DELIVERY_MODE);
            if (StringUtils.isEmpty(deliveryMode)) {
                deliveryMode = SolaceConstants.DELIVERY_MODE_DIRECT;
            }

            if (SolaceConstants.DESTINATION_TYPE_QUEUE.equalsIgnoreCase(destinationType)
                    && SolaceConstants.DELIVERY_MODE_DIRECT.equalsIgnoreCase(deliveryMode)) {
                handleException("Invalid delivery mode 'DIRECT' for QUEUE destination '" + destinationName
                        + "'. Queues require guaranteed delivery — use PERSISTENT or NON_PERSISTENT.",
                        messageContext);
                return;
            }

            // Transacted sessions only carry guaranteed messages — DIRECT bypasses the spool
            // and has no rollback semantics, so the JCSMP transacted producer rejects it.
            if (isTransactional
                    && SolaceConstants.DELIVERY_MODE_DIRECT.equalsIgnoreCase(deliveryMode)) {
                handleException("Invalid delivery mode 'DIRECT' inside a transaction (txId=" + txId
                        + ", destination='" + destinationName + "'). Transacted sessions only support"
                        + " guaranteed delivery — use PERSISTENT or NON_PERSISTENT.",
                        messageContext);
                return;
            }

            String messageType = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.MESSAGE_TYPE);
            if (StringUtils.isEmpty(messageType)) {
                messageType = SolaceConstants.MESSAGE_TYPE_TEXT;
            }
            messageType = messageType.toUpperCase();
            // Solace has no JSON message type — JSON payloads are carried as TEXT on the wire.
            // XML uses Solace's XMLContentMessage so consumers can filter on the XML SMF header.
            if (!SolaceConstants.MESSAGE_TYPE_TEXT.equals(messageType)
                    && !SolaceConstants.MESSAGE_TYPE_BYTES.equals(messageType)
                    && !SolaceConstants.MESSAGE_TYPE_XML.equals(messageType)) {
                handleException("Unsupported messageType '" + messageType
                        + "'. Supported types: TEXT, BYTES, XML.", messageContext);
                return;
            }

            // Get message payload and detect its content type for downstream subscribers
            String[] payloadAndType = SolaceUtils.extractPayloadAndContentType(messageContext);
            String payload = payloadAndType[0];
            String httpContentType = payloadAndType[1];

            if (log.isDebugEnabled()) {
                log.debug("Publishing message to Solace with parameters: destinationType=" + destinationType
                        + ", destinationName=" + destinationName + ", deliveryMode=" + deliveryMode
                        + ", messageType=" + messageType + ", httpContentType=" + httpContentType);
            }

            // Build optional message properties
            SolaceMessageProperties msgProperties = SolaceUtils.buildMessageProperties(messageContext);

            // Publish ACK wait (guaranteed mode only)
            String waitForAckStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.WAIT_FOR_ACK);
            boolean waitForAck = StringUtils.isNotEmpty(waitForAckStr) && Boolean.parseBoolean(waitForAckStr);
            String ackTimeoutStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.ACK_TIMEOUT);
            long ackTimeout = SolaceUtils.parseLongOrDefault(ackTimeoutStr,
                    SolaceConstants.DEFAULT_ACK_TIMEOUT_MS, SolaceConstants.ACK_TIMEOUT);
            String continueOnAckFailureStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.CONTINUE_ON_ACK_FAILURE);
            boolean continueOnAckFailure = StringUtils.isNotEmpty(continueOnAckFailureStr)
                    && Boolean.parseBoolean(continueOnAckFailureStr);

            if (isTransactional && waitForAck && log.isDebugEnabled()) {
                log.debug("waitForAck=true is ignored for transacted publishes — atomicity is provided by solace.commit");
            }

            // Publish the message
            try {
                PublishResult result;
                if (isTransactional) {
                    log.debug("solace.publishMessage: txId=" + txId + " sending transacted message to "
                            + destinationType + " '" + destinationName + "' (deliveryMode=" + deliveryMode
                            + ", messageType=" + messageType + ")");
                    result = connection.publishTransacted(destinationType, destinationName, payload,
                            deliveryMode, messageType, msgProperties, httpContentType, messageContext);
                    log.info("solace.publishMessage: txId=" + txId + " transacted send queued (correlationKey="
                            + result.getCorrelationKey() + ", ackStatus=" + result.getAckStatus() + ")");
                } else {
                    result = connection.publish(destinationType, destinationName, payload,
                            deliveryMode, messageType, msgProperties, waitForAck, ackTimeout,
                            httpContentType, messageContext);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Message published to " + destinationType + " '" + destinationName
                            + "' (deliveryMode=" + deliveryMode + ", payloadLength="
                            + (payload != null ? payload.length() : 0)
                            + ", ackStatus=" + result.getAckStatus() + ")");
                }

                setResultInContext(messageContext, result, destinationType, destinationName, deliveryMode,
                        messageType, responseVariable);

                if (!isTransactional && !result.isAckReceived() && waitForAck && !continueOnAckFailure
                        && (SolaceConstants.ACK_STATUS_NACK.equals(result.getAckStatus())
                        || SolaceConstants.ACK_STATUS_TIMEOUT.equals(result.getAckStatus()))) {
                    handleException("Publish failed for '" + destinationName + "': "
                            + result.getAckStatus() + " — " + result.getError(), messageContext);
                    return;
                }
            } catch (JCSMPException e) {
                handleException("Error publishing message to Solace: " + e.getMessage(), e, messageContext);
            }
        } catch (Exception e) {
            handleException("Solace publish operation failed (connection: " + connectionName + ")",
                    e, messageContext);
        } finally {
            // Pinned (transactional) connection stays held until commit/rollback.
            // Outer finally so early returns from validation also release the pooled connection.
            if (!isTransactional && connection != null) {
                handler.returnConnection(SolaceConstants.CONNECTOR_NAME, connectionName, connection);
            }
        }
    }

    private void setResultInContext(MessageContext messageContext, PublishResult result,
                                    String destinationType, String destinationName, String deliveryMode,
                                    String messageType, String responseVariable) {
        // Legacy solace.* context properties — kept for callers that read them directly.
        messageContext.setProperty(SolaceConstants.SOLACE_DESTINATION, destinationName);
        messageContext.setProperty(SolaceConstants.SOLACE_DELIVERY_MODE, deliveryMode);
        messageContext.setProperty(SolaceConstants.SOLACE_ACK_STATUS, result.getAckStatus());
        messageContext.setProperty(SolaceConstants.SOLACE_ACK_RECEIVED, String.valueOf(result.isAckReceived()));
        if (result.getCorrelationKey() != null) {
            messageContext.setProperty(SolaceConstants.SOLACE_ACK_CORRELATION_KEY, result.getCorrelationKey());
        }
        if (result.getError() != null) {
            messageContext.setProperty(SolaceConstants.SOLACE_ACK_ERROR, result.getError());
        }

        // Set the publish-result fields.
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("destination", destinationName);
        attributes.put("destinationType", destinationType);
        attributes.put("deliveryMode", deliveryMode);
        attributes.put("ackStatus", result.getAckStatus());
        attributes.put("ackReceived", result.isAckReceived());
        // correlationKey is only set for guaranteed sends and error only on failure.
        if (result.getCorrelationKey() != null) {
            attributes.put("correlationKey", result.getCorrelationKey());
        }
        if (result.getError() != null) {
            attributes.put("error", result.getError());
        }

        JSONObject response = new JSONObject(attributes);
        response.put("messageType", messageType);
        response.put("publishedAt", System.currentTimeMillis());

        // Side-effect operation: a Solace publish returns no payload, only this synthesized
        // result envelope (ack status etc.). Never overwrite the message body with it — the
        // original payload must keep flowing downstream. (overwriteBody is hidden in the UI
        // for this reason.)
        handleConnectorResponse(messageContext, responseVariable, false,
                response.toString(), null, attributes);
    }
}
