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
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.json.JSONObject;
import org.wso2.integration.connector.connection.SolaceConnection;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.models.SolaceMessageProperties;
import org.wso2.integration.connector.utils.SolaceUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Sends a reply to an inbound request message using JCSMP's native sendReply API.
 * Requires the original inbound {@link BytesXMLMessage} to be available in the message context
 * (set by the inbound endpoint under {@link SolaceConstants#SOLACE_INBOUND_MESSAGE}).
 */
public class SolaceSendReply extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(SolaceSendReply.class);

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

            // Retrieve the original inbound message for sendReply
            Object msgObj = messageContext.getProperty(SolaceConstants.SOLACE_INBOUND_MESSAGE);
            if (msgObj == null || !(msgObj instanceof BytesXMLMessage)) {
                handleException("No inbound Solace message found in message context. "
                        + "Ensure this operation is used in a request-reply flow where the inbound endpoint "
                        + "sets the '" + SolaceConstants.SOLACE_INBOUND_MESSAGE + "' property.", messageContext);
                return;
            }
            BytesXMLMessage inboundMessage = (BytesXMLMessage) msgObj;

            if (inboundMessage.getReplyTo() == null) {
                handleException("Inbound message has no reply-to destination. "
                        + "Cannot send reply to a message that is not part of a request-reply flow.",
                        messageContext);
                return;
            }

            String deliveryMode = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.DELIVERY_MODE);
            if (StringUtils.isEmpty(deliveryMode)) {
                deliveryMode = SolaceConstants.DELIVERY_MODE_DIRECT;
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

            // Get reply payload and detect its content type for downstream subscribers
            String[] payloadAndType = SolaceUtils.extractPayloadAndContentType(messageContext);
            String payload = payloadAndType[0];
            String httpContentType = payloadAndType[1];

            // Build optional message properties
            SolaceMessageProperties msgProperties = SolaceUtils.buildMessageProperties(messageContext);

            // Send the reply using JCSMP's native sendReply API
            connection.sendReply(inboundMessage, payload, deliveryMode, messageType, msgProperties,
                    httpContentType, messageContext);

            // Legacy solace.* context properties — kept for callers that read them directly.
            String replyToName = inboundMessage.getReplyTo() != null
                    ? inboundMessage.getReplyTo().getName() : "unknown";
            messageContext.setProperty(SolaceConstants.SOLACE_DESTINATION, replyToName);
            messageContext.setProperty(SolaceConstants.SOLACE_DELIVERY_MODE, deliveryMode);

            if (log.isDebugEnabled()) {
                log.debug("Reply sent to '" + replyToName + "'"
                        + (inboundMessage.getCorrelationId() != null
                                ? " with correlationId: " + inboundMessage.getCorrelationId() : ""));
            }

            // Build the response envelope so ${vars.X.payload.sent} resolves and
            // overwriteBody behaves consistently with other framework operations.
            JSONObject response = new JSONObject();
            response.put("sent", true);
            response.put("replyTo", replyToName);
            response.put("deliveryMode", deliveryMode);
            response.put("messageType", messageType);
            if (inboundMessage.getCorrelationId() != null) {
                response.put("correlationId", inboundMessage.getCorrelationId());
            }
            response.put("repliedAt", System.currentTimeMillis());

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("sent", true);
            attributes.put("replyTo", replyToName);
            attributes.put("deliveryMode", deliveryMode);
            if (inboundMessage.getCorrelationId() != null) {
                attributes.put("correlationId", inboundMessage.getCorrelationId());
            }
            // Side-effect operation: sendReply() returns void, so this is a synthesized
            // status envelope, not broker data. Never overwrite the message body with it —
            // the original payload must keep flowing downstream. (overwriteBody is hidden
            // in the UI for this reason.)
            handleConnectorResponse(messageContext, responseVariable, false,
                    response.toString(), null, attributes);

        } catch (JCSMPException e) {
            handleException("Error sending reply to Solace: " + e.getMessage(), e, messageContext);
        } catch (Exception e) {
            handleException("Failed to execute sendReply operation: " + e.getMessage(), e, messageContext);
        } finally {
            if (connection != null) {
                handler.returnConnection(SolaceConstants.CONNECTOR_NAME, connectionName, connection);
            }
        }
    }
}
