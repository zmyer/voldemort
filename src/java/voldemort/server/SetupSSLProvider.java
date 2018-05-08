package voldemort.server;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.Security;

// TODO: 2018/4/26 by zmyer
public class SetupSSLProvider {
    public static void useBouncyCastle() {
        Security.insertProviderAt(new BouncyCastleProvider(), 1);
    }
}
