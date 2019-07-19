
package io.thill.kafkacap.websocket.internal;

import io.thill.kafkacap.core.capture.BufferedPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.websocket.*;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;

@ClientEndpoint
public class WebsocketClient {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final URI endpointUri;
  private final Charset textCharset;
  private final BufferedPublisher<byte[], byte[]> bufferedPublisher;
  private volatile Session userSession;
  private volatile boolean closed = true;

  public WebsocketClient(URI endpointUri, Charset textCharset, BufferedPublisher<byte[], byte[]> bufferedPublisher) {
    this.endpointUri = endpointUri;
    this.textCharset = textCharset;
    this.bufferedPublisher = bufferedPublisher;
  }

  public void connect() throws IOException, DeploymentException {
    WebSocketContainer container = ContainerProvider.getWebSocketContainer();
    container.connectToServer(this, endpointUri);
  }

  public boolean isClosed() {
    return closed;
  }

  public void close() throws IOException {
    if(userSession != null) {
      userSession.close();
      userSession = null;
    }
  }

  @OnOpen
  public void onOpen(Session userSession) {
    logger.info("WebSocket Open. endpointUri={}", endpointUri);
    this.userSession = userSession;
    closed = false;
  }

  @OnClose
  public void onClose(Session userSession, CloseReason reason) {
    logger.info("WebSocket Closed. reason={}", reason);
    closed = true;
  }

  @OnMessage
  public void onMessage(byte[] message) {
    bufferedPublisher.write(message);
  }

  @OnMessage
  public void onMessage(String message) {
    bufferedPublisher.write(message.getBytes(textCharset));
  }

  @Override
  public String toString() {
    return "WebsocketClient{" +
            "endpoint=" + endpointUri +
            '}';
  }
}