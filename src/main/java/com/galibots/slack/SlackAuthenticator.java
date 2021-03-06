package com.galibots.slack;

import com.eclipsesource.json.JsonObject;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class SlackAuthenticator {

    // TODO: move to netty

    protected String authenticateWithSlack(String slackToken) {

        // Authenticate with Slack
        String url = "https://slack.com/api/rtm.start?token=" + slackToken;

        CloseableHttpClient httpClient = HttpClients.custom()
                .addInterceptorFirst(new HttpRequestInterceptor() {

                    public void process(
                            final HttpRequest request,
                            final HttpContext context) throws HttpException, IOException {
                        if (!request.containsHeader("Accept-Encoding")) {
                            request.addHeader("Accept-Encoding", "gzip");
                        }

                    }
                }).addInterceptorFirst(new HttpResponseInterceptor() {

                    public void process(
                            final HttpResponse response,
                            final HttpContext context) throws HttpException, IOException {
                        HttpEntity entity = response.getEntity();
                        if (entity != null) {
                            Header ceheader = entity.getContentEncoding();
                            if (ceheader != null) {
                                HeaderElement[] codecs = ceheader.getElements();
                                for (int i = 0; i < codecs.length; i++) {
                                    if (codecs[i].getName().equalsIgnoreCase("gzip")) {
                                        response.setEntity(
                                                new GzipDecompressingEntity(response.getEntity()));
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }).build();

        try {
            HttpUriRequest query = RequestBuilder.get()
                    .setUri(url)
                    .build();
            CloseableHttpResponse queryResponse = httpClient.execute(query);
            try {
                HttpEntity entity = queryResponse.getEntity();
                if (entity != null) {
                    String data = EntityUtils.toString(entity);
                    JsonObject jsonObject = JsonObject.readFrom(data);
                    Boolean result = jsonObject.get("ok").asBoolean();
                    String wssUrl = jsonObject.get("url").asString();

                    if (result) {
                        return wssUrl;
                    }
                }

                return null;

            } finally {
                queryResponse.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                httpClient.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
