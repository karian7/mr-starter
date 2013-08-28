package com.daumcorp.platform.hadoop;

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.junit.Test;

/**
 * @author karian7
 * @since 13. 8. 27.
 */
public class MyJobTest {
	@Test
	public void parseUserid() throws Exception {
		assertThat(parseUserid("4WeLgWeLg"), is("WeLg"));
		assertThat(parseUserid("54CO6O48Iyv"), is("48Iyv"));

	}

	@Test
	public void parseTime() throws Exception {
		String dateStr = "[27/Aug/2013 00:05:47";
		SimpleDateFormat dateFormat = new SimpleDateFormat("[dd/MMM/yyyy HH:mm:ss", Locale.ENGLISH);
		Date date = dateFormat.parse(dateStr);
		assertThat("2013/08/27 00:05:47", is(DateFormatUtils.format(date, "yyyy/MM/dd HH:mm:ss")));
	}

	private String parseUserid(String encUserid) {
		int idLength = NumberUtils.toInt(encUserid.substring(0, 1));
		int idStartIndex = encUserid.length() - idLength;
		return encUserid.substring(idStartIndex);
	}
}
