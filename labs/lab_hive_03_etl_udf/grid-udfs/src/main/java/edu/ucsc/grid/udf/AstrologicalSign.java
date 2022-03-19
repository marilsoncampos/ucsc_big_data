/*
  Copyright (c) 2018.
  Project : Astrological Sign Simple Hive UDF.
  User : Marilson
  Date : 03/18/18
 */
package edu.ucsc.grid.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;


/**
 * Astrological Sign for multiple countries.
 */
@Description(name = "astro_sign",
    value = "_FUNC_(dob) - Returns the astrological sign ",
    extended = "dob is the date of birth ('yyyy-MM-dd') of the person.\n"
        + "Example:\n "
        + "  > SELECT _FUNC_('2001-11-11');\n Scorpio")
public class AstrologicalSign extends UDF {
  private static final String ARIES = "Aries";
  private static final String TAURUS = "Taurus";
  private static final String GEMINI = "Gemini";
  private static final String CANCER = "Cancer";
  private static final String LEO = "Leo";
  private static final String VIRGO = "Virgo";
  private static final String LIBRA = "Libra";
  private static final String SCORPIO = "Scorpio";
  private static final String SAGITTARIUS = "Sagittarius";
  private static final String CAPRICORN = "Capricorn";
  private static final String AQUARIUS = "Aquarius";
  private static final String PISCES = "Pisces";
  private static final String UNK_EXCEPT = "_UNK_EXCEPT";
  // Members
  private Text result;

  public AstrologicalSign() {
    this.result = new Text();
  }


  /**
   * Get the sign from date of birth and country code.
   *
   * @param dob the date of birth as "yyyy-MM-dd".
   * @return an Text with the name of the astrological sign or null if not
   * valid.
   */
  public Text evaluate2(Text dob) {

    if (dob == null) {
      result.set("");
      return result;
    }
    try {
      String dobStr = dob.toString();
      String monthDay = dobStr.substring(5);
      result.set(monthDay);
      return result;
    } catch (Exception e) {
      result.set(UNK_EXCEPT);
      return result;
    }
  }

  public Text evaluate(Text dob) {
    if (dob == null) {
      result.set("");
      return result;
    }
    try {
      String dobStr = dob.toString();
      String monthDay = dobStr.substring(5);

      if ((monthDay.compareTo("03-21") >= 0) && (monthDay.compareTo("04-19") <= 0)) {
        result.set(ARIES);
        return result;
      }
      if ((monthDay.compareTo("04-20") >= 0) && (monthDay.compareTo("05-20") <= 0)) {
        result.set(TAURUS);
        return result;
      }
      if ((monthDay.compareTo("05-21") >= 0) && (monthDay.compareTo("06-21") <= 0)) {
        result.set(GEMINI);
        return result;
      }
      if ((monthDay.compareTo("06-22") >= 0) && (monthDay.compareTo("07-22") <= 0)) {
        result.set(CANCER);
        return result;
      }
      if ((monthDay.compareTo("07-23") >= 0) && (monthDay.compareTo("08-22") <= 0)) {
        result.set(LEO);
        return result;
      }
      if ((monthDay.compareTo("08-23") >= 0) && (monthDay.compareTo("09-22") <= 0)) {
        result.set(VIRGO);
        return result;
      }
      if ((monthDay.compareTo("09-23") >= 0) && (monthDay.compareTo("10-21") <= 0)) {
        result.set(LIBRA);
        return result;
      }
      if ((monthDay.compareTo("10-23") >= 0) && (monthDay.compareTo("11-21") <= 0)) {
        result.set(SCORPIO);
        return result;
      }
      if ((monthDay.compareTo("11-22") >= 0) && (monthDay.compareTo("12-21") <= 0)) {
        result.set(SAGITTARIUS);
        return result;
      }
      if ((monthDay.compareTo("12-22") >= 0) && (monthDay.compareTo("12-31") <= 0)) {
        result.set(CAPRICORN);
        return result;
      }
      if ((monthDay.compareTo("01-01") >= 0) && (monthDay.compareTo("01-19") <= 0)) {
        result.set(CAPRICORN);
        return result;
      }
      if ((monthDay.compareTo("01-20") >= 0) && (monthDay.compareTo("02-18") <= 0)) {
        result.set(AQUARIUS);
        return result;
      }
      if ((monthDay.compareTo("02-19") >= 0) && (monthDay.compareTo("03-20") <= 0)) {
        result.set(PISCES);
        return result;
      }
    } catch (Exception e) {
      result.set(UNK_EXCEPT);
      return result;
    }
    result.set(UNK_EXCEPT);
    return result;
  }
}
