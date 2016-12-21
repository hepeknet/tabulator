package net.hepek.tabulator;

import java.security.MessageDigest;

public abstract class StringUtil {

	public static String getMD5Hash(String str) {
		if(str == null){
			throw new IllegalArgumentException("Input must not be null");
		}
		try {
			final byte[] bytesOfMessage = str.getBytes("UTF-8");
			final MessageDigest md = MessageDigest.getInstance("MD5");
			final byte[] thedigest = md.digest(bytesOfMessage);
			final StringBuffer sb = new StringBuffer();
			for (final byte b : thedigest) {
				sb.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1, 3));
			}
			return sb.toString();
		} catch (final Exception exc) {
			throw new IllegalStateException(
					"Was not able to calculate digest for [" + str + "] - details " + exc.getMessage());
		}
	}

}
