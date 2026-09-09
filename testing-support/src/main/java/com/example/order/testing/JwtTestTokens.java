package com.example.order.testing;

import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.List;

/** Local RSA keypair plus a tiny JWKS endpoint. Development only. */
public final class JwtTestTokens {

    private static final RSAKey KEY;
    private static final HttpServer JWKS_SERVER;

    static {
        try {
            KEY = new RSAKeyGenerator(2048).keyID("test-key").generate();
            JWKS_SERVER = HttpServer.create(new InetSocketAddress(0), 0);
            JWKS_SERVER.createContext("/jwks", exchange -> {
                byte[] body = ("{\"keys\":[" + KEY.toPublicJWK().toJSONString() + "]}")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            JWKS_SERVER.start();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start JWKS fixture", e);
        }
    }

    private JwtTestTokens() {
    }

    public static String jwkSetUri() {
        return "http://localhost:" + JWKS_SERVER.getAddress().getPort() + "/jwks";
    }

    public static String signedToken(String subject, List<String> scopes) {
        try {
            var claims = new JWTClaimsSet.Builder()
                    .subject(subject)
                    .issuer("https://dev.example.com")
                    .audience("order-api")
                    .claim("scope", String.join(" ", scopes))
                    .issueTime(Date.from(Instant.now()))
                    .expirationTime(Date.from(Instant.now().plusSeconds(600)))
                    .build();
            var jwt = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(KEY.getKeyID()).build(), claims);
            jwt.sign(new RSASSASigner(KEY));
            return jwt.serialize();
        } catch (JOSEException e) {
            throw new IllegalStateException("Failed to sign test token", e);
        }
    }
}
