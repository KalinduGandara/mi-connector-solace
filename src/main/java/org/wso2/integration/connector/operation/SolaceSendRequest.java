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

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPRequestTimeoutException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.wso2.integration.connector.connection.SolaceConnection;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.models.SolaceMessageProperties;
import org.wso2.integration.connector.utils.SolaceUtils;

/**
 * Sends a request message and blocks until a reply arrives or the timeout expires.
 * DIRECT delivery uses the JCSMP {@code Requestor}; PERSISTENT/NON_PERSISTENT delivery
 * uses a temporary queue with a {@code FlowReceiver} for guaranteed request-reply.
 */
public class SolaceSendRequest extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(SolaceSendRequest.class);
    private static final long DEFAULT_TIMEOUT_MS = 30000L;

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = (String) messageContext.getProperty(SolaceConstants.NAME);
        if (connectionName == null) {
            handleException("Connection name is not set.", messageContext);
            return;
        }

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
        SolaceConnection connection = null;

        try {
            connection = (SolaceConnection) handler.getConnection(SolaceConstants.CONNECTOR_NAME, connectionName);
            if (connection == null || !connection.isConnected()) {
                handleException("Solace connection is not available or not connected.", messageContext);
                return;
            }

            // Extract parameters
            String destinationType = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.DESTINATION_TYPE);
            if (StringUtils.isEmpty(destinationType)) {
                destinationType = SolaceConstants.DESTINATION_TYPE_TOPIC;
            }

            String destinationName = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.DESTINATION_NAME);
            if (StringUtils.isEmpty(destinationName)) {
                handleException("Destination name is required for sendRequest operation.", messageContext);
                return;
            }

            String deliveryMode = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.DELIVERY_MODE);
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

            String messageType = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.MESSAGE_TYPE);
            if (StringUtils.isEmpty(messageType)) {
                messageType = SolaceConstants.MESSAGE_TYPE_TEXT;
            }
            messageType = messageType.toUpperCase();
            // Solace has no JSON message type — JSON payloads are carried as TEXT on the wire.
            if (!SolaceConstants.MESSAGE_TYPE_TEXT.equals(messageType)
                    && !SolaceConstants.MESSAGE_TYPE_BYTES.equals(messageType)
                    && !SolaceConstants.MESSAGE_TYPE_XML.equals(messageType)) {
                handleException("Unsupported messageType '" + messageType
                        + "'. Supported types: TEXT, BYTES, XML.", messageContext);
                return;
            }

            String timeoutStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.REQUEST_TIMEOUT);
            long timeout = SolaceUtils.parseLongOrDefault(timeoutStr, DEFAULT_TIMEOUT_MS,
                    SolaceConstants.REQUEST_TIMEOUT);

            // Get message payload and detect its content type for downstream subscribers
            String[] payloadAndType = SolaceUtils.extractPayloadAndContentType(messageContext);
            String payload = payloadAndType[0];
            String httpContentType = payloadAndType[1];

            // Build optional message properties
            SolaceMessageProperties msgProperties = SolaceUtils.buildMessageProperties(messageContext);

            // Send the request (DIRECT via Requestor, guaranteed via temp queue + FlowReceiver)
            BytesXMLMessage response;
            try {
                response = connection.sendRequest(destinationType, destinationName, payload, deliveryMode,
                        messageType, msgProperties, timeout, httpContentType, messageContext);
            } catch (JCSMPRequestTimeoutException e) {
                handleException("sendRequest timed out after " + timeout + "ms. No response received from '"
                        + destinationName + "'.", e, messageContext);
                return;
            }

            if (response == null) {
                handleException("sendRequest returned no response from '" + destinationName + "'.",
                        messageContext);
                return;
            }

            // Extract raw payload bytes from response
            byte[] responseBytes = SolaceUtils.extractRawPayload(response);
            String contentType = response.getHTTPContentType();
            if (contentType == null || contentType.isEmpty()) {
                contentType = SolaceUtils.inferContentType(response);
            }

            // Legacy solace.* context properties — kept for callers that read them directly.
            messageContext.setProperty(SolaceConstants.SOLACE_DESTINATION, destinationName);
            messageContext.setProperty(SolaceConstants.SOLACE_DELIVERY_MODE, deliveryMode);
            if (response.getCorrelationId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_CORRELATION_ID, response.getCorrelationId());
            }
            if (response.getSenderId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_SENDER_ID, response.getSenderId());
            }
            if (response.getApplicationMessageId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_APPLICATION_MESSAGE_ID,
                        response.getApplicationMessageId());
            }
            if (response.getApplicationMessageType() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_APPLICATION_MESSAGE_TYPE,
                        response.getApplicationMessageType());
            }
            if (response.getReceiveTimestamp() > 0) {
                messageContext.setProperty(SolaceConstants.SOLACE_REPLY_RECEIVE_TIMESTAMP,
                        response.getReceiveTimestamp());
            }

            // Build attributes (the metadata) and the payload (the reply content) and route
            // both through handleConnectorResponse so ${vars.X.payload} works and the
            // overwriteBody flag is honored consistently with other framework operations.
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("destination", destinationName);
            attributes.put("destinationType", destinationType);
            attributes.put("deliveryMode", deliveryMode);
            attributes.put("contentType", contentType);
            if (response.getCorrelationId() != null) {
                attributes.put("correlationId", response.getCorrelationId());
            }
            if (response.getApplicationMessageId() != null) {
                attributes.put("applicationMessageId", response.getApplicationMessageId());
            }
            if (response.getApplicationMessageType() != null) {
                attributes.put("applicationMessageType", response.getApplicationMessageType());
            }
            if (response.getReceiveTimestamp() > 0) {
                attributes.put("receiveTimestamp", response.getReceiveTimestamp());
            }

            String body = new String(responseBytes, StandardCharsets.UTF_8);
            String payloadJson = SolaceUtils.looksLikeJson(body) ? body : JSONObject.quote(body);

            handleConnectorResponse(messageContext, responseVariable, overwriteBody,
                    payloadJson, null, attributes);

            if (log.isDebugEnabled()) {
                log.debug("Response received successfully for request to " + destinationName);
            }

        } catch (JCSMPException e) {
            handleException("Failed to execute sendRequest operation: " + e.getMessage(), e, messageContext);
        } catch (Exception e) {
            handleException("Failed to execute sendRequest operation: " + e.getMessage(), e, messageContext);
        } finally {
            if (connection != null) {
                handler.returnConnection(SolaceConstants.CONNECTOR_NAME, connectionName, connection);
            }
        }
    }

    // Payload extraction / content-type inference moved to SolaceUtils (shared with the
    // poll, browse, and requestCachedMessages operations).
}
