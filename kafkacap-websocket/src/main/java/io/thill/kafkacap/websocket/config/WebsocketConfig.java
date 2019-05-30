/*
 * Licensed under the Apache License, Version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.thill.kafkacap.websocket.config;

public class WebsocketConfig {
  private String url;
  private String charset = "UTF-8";

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getCharset() {
    return charset;
  }

  public void setCharset(String charset) {
    this.charset = charset;
  }

  @Override
  public String toString() {
    return "WebsocketConfig{" +
            "url='" + url + '\'' +
            ", charset='" + charset + '\'' +
            '}';
  }
}
