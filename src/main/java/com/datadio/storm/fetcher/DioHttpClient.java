package com.datadio.storm.fetcher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Random;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnRouteParams;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DecompressingHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

public class DioHttpClient {
    
    private CookieStore cookieStore = new BasicCookieStore();
    private HttpContext localContext = new BasicHttpContext(); 
    
    public byte[] exec(String url, String ua) throws IOException, URISyntaxException {
        
        HttpParams httpsParams = new BasicHttpParams();
        DecompressingHttpClient httpClient = new DecompressingHttpClient(DioHttpClient.wrapClient(new DefaultHttpClient(httpsParams)));
        
        HttpConnectionParams.setConnectionTimeout(httpsParams, 3000); 
        HttpConnectionParams.setSoTimeout(httpsParams, 3000);
        
        // Set User-Agent
        httpClient.getParams().setParameter(CoreProtocolPNames.USER_AGENT, "Mozilla/5.0 (compatible;" + ua + "/1.7; +https://www.Datadio.com)");

        URL currentURL = new URL(url);
        HttpGet method = new HttpGet(currentURL.toURI());
        
        // Set the Accept header - this'll get us past Bad Behavior Plugin, etc.
        method.setHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
        
//                    ArrayList<byte[]> ip_addrs = new ArrayList<byte[]>();
//                    ip_addrs.add(new byte[]{ (byte)198, (byte)178, (byte)121, (byte)115 });
//                    ip_addrs.add(new byte[]{ (byte)198, (byte)178, (byte)121, (byte)122 });
//                    ip_addrs.add(new byte[]{ (byte)198, (byte)178, (byte)121, (byte)123 });
//                    ip_addrs.add(new byte[]{ (byte)198, (byte)178, (byte)121, (byte)124 });
//                    ip_addrs.add(new byte[]{ (byte)198, (byte)178, (byte)121, (byte)125 });
//
//                    Random rand = new Random();
//
//                    // Set the IP address randomly.
//                    ConnRouteParams.setLocalAddress(httpsParams, InetAddress.getByAddress(ip_addrs.get(rand.nextInt(ip_addrs.size()))));
        
        HttpResponse response = httpClient.execute(method,this.localContext);
        InputStream is = response.getEntity().getContent();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        byte[] buffer = new byte[1024];
        int nbRead;
        while ((nbRead = is.read(buffer)) != -1) {
            bos.write(buffer, 0, nbRead);
        }

        is.close();

        
        method.releaseConnection();
        httpClient.getConnectionManager().shutdown();

        return bos.toByteArray();
    }
    
    public static DefaultHttpClient wrapClient(HttpClient base) {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            X509TrustManager tm = new X509TrustManager() {
 
                public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
                }
 
                public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
                }
 
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }
            };
            X509HostnameVerifier verifier = new X509HostnameVerifier() {
 
                public void verify(String string, SSLSocket ssls) throws IOException {
                }
 
                public void verify(String string, X509Certificate xc) throws SSLException {
                }
 
                public void verify(String string, String[] strings, String[] strings1) throws SSLException {
                }
 
                public boolean verify(String string, SSLSession ssls) {
                    return true;
                }
            };
            ctx.init(null, new TrustManager[]{tm}, null);
            SSLSocketFactory ssf = new SSLSocketFactory(ctx);
            ssf.setHostnameVerifier(verifier);
            ClientConnectionManager ccm = base.getConnectionManager();
            SchemeRegistry sr = ccm.getSchemeRegistry();
            sr.register(new Scheme("https", ssf, 443));
            
            return new DefaultHttpClient(ccm, base.getParams());
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }
    
}
