# Build XCC first.
# You must, however, have Java 1.4 installed.  The ifeq tests
# below will attempt to deduce the location of the JDK.  If
# the JDK is not in one of these locations on your system, then
# explicitly set the value of JAVA_HOME (the directory that
# contains bin/java).

TOP=$(shell pwd)

ANT_NAME=apache-ant-1.7.1
ANT_TAR=$(ANT_NAME)-bin.tar.gz

SRC=$(TOP)/src
DELIVERABLE=$(TOP)/deliverable
BUILD_TMP=$(TOP)/buildtmp
BUILD_ANT=$(TOP)/ant
DIST_SRC=$(SRC)/dist
ANT_DIST=$(DIST_SRC)/$(ANT_TAR)
ANT_HOME=$(BUILD_ANT)/$(ANT_NAME)

ifneq "$(WINDIR)" ""
  OS_NAME=winnt
  WINDIRU:=$(shell cygpath --unix '$(WINDIR)')
else
  OS_NAME=$(shell uname -s)
endif

ANT=$(ANT_HOME)/bin/ant -lib src/lib/junit.jar -lib src/lib/ant-ikvmc.0.5.jar

ifeq "$(JAVA_HOME)" ""
  JAVAC=$(shell which javac)
  ifneq "$(JAVAC)" ""
      JAVA_BIN=$(shell dirname '$(JAVAC)')
      JAVA_HOME=$(shell dirname '$(JAVA_BIN)')
  else
    ifeq ($(OS_NAME),Darwin)
      JAVA_HOME=/Library/Java/Home
    else
      ifeq ($(OS_NAME),Linux)
        JAVA_HOME=/usr/java/j2sdk1.4.2_11
      else
        JAVA_HOME=/usr/local/java
      endif
    endif
  endif
endif

export CLASSPATH=
export JAVA_HOME
export ANT_HOME

# -------------------------------------------------

all: $(ANT_HOME)
	$(ANT) all

mapreduce: $(ANT_HOME)
	$(ANT) mapreduce

package: $(ANT_HOME)
	$(ANT) package

test: $(ANT_HOME)
	$(ANT) test

clean:
	rm -rf $(BUILD_TMP)

cleanest:
	rm -rf $(BUILD_TMP)
	rm -rf $(BUILD_ANT)
	rm -rf $(DELIVERABLE)

# -------------------------------------------------

$(ANT_HOME):
	mkdir -p $@
	tar xzCf $(BUILD_ANT) $(ANT_DIST)

# -------------------------------------------------


