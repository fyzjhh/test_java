package com.test.servyou;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Map;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
public class SignatureUtils {
    private final static String CHARSET_UTF8 = "utf8";
    private final static String ALGORITHM = "UTF-8";
    private final static String SEPARATOR = "&";
    public static Map<String, String> splitQueryString(String url)
            throws URISyntaxException, UnsupportedEncodingException {
        URI uri = new URI(url);
        String query = uri.getQuery();
        final String[] pairs = query.split("&");
        TreeMap<String, String> queryMap = new TreeMap<String, String>();
        for (String pair : pairs) {
            final int idx = pair.indexOf("=");
            final String key = idx > 0 ? pair.substring(0, idx) : pair;
            if (!queryMap.containsKey(key)) {
                queryMap.put(key, URLDecoder.decode(pair.substring(idx + 1), CHARSET_UTF8));
            }
        }
        return queryMap;
    }
    public static String generate(String method, Map<String, String> parameter,
            String accessKeySecret) throws Exception {
        String signString = generateSignString(method, parameter);
        System.out.println("signString---"+signString);
        byte[] signBytes = hmacSHA1Signature(accessKeySecret + "&", signString);
        String signature = newStringByBase64(signBytes);
        System.out.println("signature---"+signature);
        if ("POST".equals(method))
            return signature;
        return URLEncoder.encode(signature, "UTF-8");
    }
    public static String generateSignString(String httpMethod, Map<String, String> parameter)
            throws IOException {
        TreeMap<String, String> sortParameter = new TreeMap<String, String>();
        sortParameter.putAll(parameter);
        String canonicalizedQueryString = UrlUtil.generateQueryString(sortParameter, true);
        if (null == httpMethod) {
            throw new RuntimeException("httpMethod can not be empty");
        }
        StringBuilder stringToSign = new StringBuilder();
        stringToSign.append(httpMethod).append(SEPARATOR);
        stringToSign.append(percentEncode("/")).append(SEPARATOR);
        stringToSign.append(percentEncode(canonicalizedQueryString));
        return stringToSign.toString();
    }
    public static String percentEncode(String value) {
        try {
            return value == null ? null : URLEncoder.encode(value, CHARSET_UTF8)
                    .replace("+", "%20").replace("*", "%2A").replace("%7E", "~");
        } catch (Exception e) {
        }
        return "";
    }
    public static byte[] hmacSHA1Signature(String secret, String baseString)
            throws Exception {
        if (StringUtils.isEmpty(secret)) {
            throw new IOException("secret can not be empty");
        }
        if (StringUtils.isEmpty(baseString)) {
            return null;
        }
        Mac mac = Mac.getInstance("HmacSHA1");
        SecretKeySpec keySpec = new SecretKeySpec(secret.getBytes(CHARSET_UTF8), ALGORITHM);
        mac.init(keySpec);
        return mac.doFinal(baseString.getBytes(CHARSET_UTF8));
    }
    public static String newStringByBase64(byte[] bytes)
            throws UnsupportedEncodingException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return new String(Base64.encodeBase64(bytes, false), CHARSET_UTF8);
    }
    public static void main(String[] args) {
        String str = "GET&%2F&AccessKeyId%3DCdwKFNmXeHJuMOrT%26Action%3DDescribeInstances%26Format%3DJSON%26RegionId%3Dcn-hangzhou%26SignatureMethod%3DHMAC-SHA1%26SignatureNonce%3D9fdf20f2-9a32-4872-bcd4-c6036082ebef%26SignatureVersion%3D1.0%26Timestamp%3D2015-12-21T09%253A05%253A44Z%26Version%3D2014-05-26";
        byte[] signBytes;
        try {
            signBytes = SignatureUtils.hmacSHA1Signature("byczfpx4PKBzUNjjL4261cE3s6HQmH" + "&", str.toString());
            String signature = SignatureUtils.newStringByBase64(signBytes);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}