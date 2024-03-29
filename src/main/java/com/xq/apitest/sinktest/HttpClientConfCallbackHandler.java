
package com.xq.apitest.sinktest;

import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.KerberosCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.common.settings.SecureString;
import org.ietf.jgss.*;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class HttpClientConfCallbackHandler implements HttpClientConfigCallback {
    private static final String SUN_KRB5_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";
    private static final String CRED_CONF_NAME = "ESClientLoginConf";
    private static final Oid SPNEGO_OID = getSpnegoOid();

    private static Oid getSpnegoOid() {
        Oid oid = null;
        try {
            oid = new Oid("1.3.6.1.5.5.2");
        } catch (GSSException gsse) {
            throw ExceptionsHelper.convertToRuntime(gsse);
        }
        return oid;
    }

    private final String userPrincipalName;
    private final SecureString password;
    private final String keytabPath;
    private final boolean enableDebugLogs;
    private LoginContext loginContext;

    /**
     * principalName and password.
     *
     * @param userPrincipalName user principal name
     * @param password password for user
     * @param enableDebugLogs if {@code true} enables kerberos debug logs
     */
    public HttpClientConfCallbackHandler(final String userPrincipalName, final SecureString password,
                                         final boolean enableDebugLogs) {
        this.userPrincipalName = userPrincipalName;
        this.password = password;
        this.keytabPath = null;
        this.enableDebugLogs = enableDebugLogs;
    }

    /**
     * principalName and keytab.
     *
     * @param principal User principal name
     * @param keytabPath path to keytab file for user
     * @param enableDebugLogs if {@code true} enables kerberos debug logs
     */
    public HttpClientConfCallbackHandler(final String principal, final String keytabPath, final boolean enableDebugLogs) {
        this.userPrincipalName = principal;
        this.keytabPath = keytabPath;
        this.password = null;
        this.enableDebugLogs = enableDebugLogs;
    }

    @Override
    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
        setupAuthSchemeSupport(httpClientBuilder);
        return httpClientBuilder;
    }

    private void setupAuthSchemeSupport(HttpAsyncClientBuilder httpClientBuilder) {
        final Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create()
                .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory()).build();

        final GSSManager gssManager = GSSManager.getInstance();
        try {
            final GSSName gssUserPrincipalName = gssManager.createName(userPrincipalName, GSSName.NT_USER_NAME);
            login();
            final AccessControlContext acc = AccessController.getContext();
            final GSSCredential credential = doAsPrivilegedWrapper(loginContext.getSubject(),
                    (PrivilegedExceptionAction<GSSCredential>) () -> gssManager.createCredential(gssUserPrincipalName,
                            GSSCredential.DEFAULT_LIFETIME, SPNEGO_OID, GSSCredential.INITIATE_ONLY),
                    acc);

            final KerberosCredentialsProvider credentialsProvider = new KerberosCredentialsProvider();
            credentialsProvider.setCredentials(
                    new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, AuthSchemes.SPNEGO),
                    new KerberosCredentials(credential));
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        } catch (GSSException e) {
            throw new RuntimeException(e);
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e.getCause());
        }
        httpClientBuilder.setDefaultAuthSchemeRegistry(authSchemeRegistry);
    }

    /**
     * If logged in {@link LoginContext} is not available, it attempts login and
     * returns {@link LoginContext}
     *
     * @return {@link LoginContext}
     * @throws PrivilegedActionException
     */
    public synchronized LoginContext login() throws PrivilegedActionException {
        if (this.loginContext == null) {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                final Subject subject = new Subject(false, Collections.singleton(new KerberosPrincipal(userPrincipalName)),
                        Collections.emptySet(), Collections.emptySet());
                Configuration conf = null;
                final CallbackHandler callback;
                if (password != null) {
                    conf = new PasswordJaasConf(userPrincipalName, enableDebugLogs);
                    callback = new KrbCallbackHandler(userPrincipalName, password);
                } else {
                    conf = new KeytabJaasConf(userPrincipalName, keytabPath, enableDebugLogs);
                    callback = null;
                }
                loginContext = new LoginContext(CRED_CONF_NAME, subject, callback, conf);
                loginContext.login();
                return null;
            });
        }
        return loginContext;
    }

    /**
     * Privileged Wrapper that invokes action with Subject.doAs to perform work as
     * given subject.
     *
     * @param subject {@link Subject} to be used for this work
     * @param action {@link PrivilegedExceptionAction} action for performing inside
     *            Subject.doAs
     * @param acc the {@link AccessControlContext} to be tied to the specified
     *            subject and action see
     *            {@link Subject#doAsPrivileged(Subject, PrivilegedExceptionAction, AccessControlContext)
     * @return the value returned by the PrivilegedExceptionAction's run method
     * @throws PrivilegedActionException
     */
    static <T> T doAsPrivilegedWrapper(final Subject subject, final PrivilegedExceptionAction<T> action, final AccessControlContext acc)
            throws PrivilegedActionException {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> Subject.doAsPrivileged(subject, action, acc));
        } catch (PrivilegedActionException pae) {
            if (pae.getCause() instanceof PrivilegedActionException) {
                throw (PrivilegedActionException) pae.getCause();
            }
            throw pae;
        }
    }

    /**
     * This class matches {@link AuthScope} and based on that returns
     * {@link Credentials}. Only supports {@link AuthSchemes#SPNEGO} in
     * {@link AuthScope#getScheme()}
     */
    private static class KerberosCredentialsProvider implements CredentialsProvider {
        private AuthScope authScope;
        private Credentials credentials;

        @Override
        public void setCredentials(AuthScope authscope, Credentials credentials) {
            if (authscope.getScheme().regionMatches(true, 0, AuthSchemes.SPNEGO, 0, AuthSchemes.SPNEGO.length()) == false) {
                throw new IllegalArgumentException("Only " + AuthSchemes.SPNEGO + " auth scheme is supported in AuthScope");
            }
            this.authScope = authscope;
            this.credentials = credentials;
        }

        @Override
        public Credentials getCredentials(AuthScope authscope) {
            assert this.authScope != null && authscope != null;
            return authscope.match(this.authScope) > -1 ? this.credentials : null;
        }

        @Override
        public void clear() {
            this.authScope = null;
            this.credentials = null;
        }
    }

    /**
     * Jaas call back handler to provide credentials.
     */
    private static class KrbCallbackHandler implements CallbackHandler {
        private final String principal;
        private final SecureString password;

        KrbCallbackHandler(final String principal, final SecureString password) {
            this.principal = principal;
            this.password = password;
        }

        public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof PasswordCallback) {
                    PasswordCallback pc = (PasswordCallback) callback;
                    if (pc.getPrompt().contains(principal)) {
                        pc.setPassword(password.getChars());
                        break;
                    }
                }
            }
        }
    }

    /**
     * Usually we would have a JAAS configuration file for login configuration.
     * Instead of an additional file setting as we do not want the options to be
     * customizable we are constructing it in memory.
     * <p>
     * As we are using this instead of jaas.conf, this requires refresh of
     * {@link Configuration} and reqires appropriate security permissions to do so.
     */
    private static class PasswordJaasConf extends AbstractJaasConf {

        PasswordJaasConf(final String userPrincipalName, final boolean enableDebugLogs) {
            super(userPrincipalName, enableDebugLogs);
        }

        public void addOptions(final Map<String, String> options) {
            options.put("useTicketCache", Boolean.FALSE.toString());
            options.put("useKeyTab", Boolean.FALSE.toString());
        }
    }

    /**
     * Usually we would have a JAAS configuration file for login configuration. As
     * we have static configuration except debug flag, we are constructing in
     * memory. This avoids additional configuration required from the user.
     * <p>
     * As we are using this instead of jaas.conf, this requires refresh of
     * {@link Configuration} and requires appropriate security permissions to do so.
     */
    private static class KeytabJaasConf extends AbstractJaasConf {
        private final String keytabFilePath;

        KeytabJaasConf(final String userPrincipalName, final String keytabFilePath, final boolean enableDebugLogs) {
            super(userPrincipalName, enableDebugLogs);
            this.keytabFilePath = keytabFilePath;
        }

        public void addOptions(final Map<String, String> options) {
            options.put("useKeyTab", Boolean.TRUE.toString());
            options.put("keyTab", keytabFilePath);
            options.put("doNotPrompt", Boolean.TRUE.toString());
        }

    }

    private abstract static class AbstractJaasConf extends Configuration {
        private final String userPrincipalName;
        private final boolean enableDebugLogs;

        AbstractJaasConf(final String userPrincipalName, final boolean enableDebugLogs) {
            this.userPrincipalName = userPrincipalName;
            this.enableDebugLogs = enableDebugLogs;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(final String name) {
            final Map<String, String> options = new HashMap<>();
            options.put("principal", userPrincipalName);
            options.put("isInitiator", Boolean.TRUE.toString());
            options.put("storeKey", Boolean.TRUE.toString());
            options.put("debug", Boolean.toString(enableDebugLogs));
            addOptions(options);
            return new AppConfigurationEntry[] { new AppConfigurationEntry(SUN_KRB5_LOGIN_MODULE,
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, Collections.unmodifiableMap(options)) };
        }

        abstract void addOptions(Map<String, String> options);
    }
}
