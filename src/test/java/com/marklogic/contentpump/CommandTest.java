package com.marklogic.contentpump;

import com.marklogic.mapreduce.MarkLogicConstants;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CommandTest {

  @Test
  public void isNullOrEqualsTrueWithNull() {
    String arg = null;
    assertTrue(Command.isNullOrEqualsTrue(arg));
  }

  @Test
  public void isNullOrEqualsTrueWithTrue() {
    assertTrue(Command.isNullOrEqualsTrue("true"));
  }

  @Test
  public void isNullOrEqualsTrueWithTrueCaseInsensitive() {
    assertTrue(Command.isNullOrEqualsTrue("TRUE"));
  }

  @Test
  public void isNullOrEqualsTrueWithFalse() {
    assertFalse(Command.isNullOrEqualsTrue("false"));
  }

  @Test
  public void setProtocol() {
    String[] values = {"TLS", "TLSv1", "TLSv1.1", "TLSv1.2"};
    //verify each of the supported protocols
    for (String value : values) {
      Configuration conf = setProtocol(value);
      verify(conf, times(1)).set(anyString(), anyString());
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void setProtocolWithInvalidProtocol() {
    String value = "TLSvInvalid";
    Configuration conf = setProtocol(value);
    verify(conf, times(0)).set(anyString(), anyString());
  }

  private Configuration setProtocol(String value) {
    String option = ConfigConstants.OUTPUT_SSL_PROTOCOL;
    String protocol = MarkLogicConstants.OUTPUT_SSL_PROTOCOL;

    Configuration conf = mock(Configuration.class);
    CommandLine cmdline = mock(CommandLine.class);
    when(cmdline.hasOption(option)).thenReturn(true);
    when(cmdline.getOptionValue(option)).thenReturn(value);

    Command.applyProtocol(conf, cmdline, option, protocol);
    return conf;
  }

  @Test
  public void setProtocolWithInvalidOption() {
    String option = "invalidOption";
    String protocol = MarkLogicConstants.OUTPUT_SSL_PROTOCOL;

    Configuration conf = mock(Configuration.class);
    CommandLine cmdline = mock(CommandLine.class);
    when(cmdline.hasOption(option)).thenReturn(false);

    Command.applyProtocol(conf, cmdline, option, protocol);
    verify(conf, times(0)).set(anyString(), anyString());
  }

}