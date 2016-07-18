@ECHO OFF
set argss=%*

set cmdpath=%~dp0
REM echo ******* Command Path:  %cmdpath%
set cmdpath=%cmdpath:~0,-1%
REM echo ******* Command Path 2:  %cmdpath%

for %%d in ("%cmdpath%") do set cmdppath=%%~dpd
REM echo ******* Command Parent Path: %cmdppath%

set LIB_HOME=%cmdppath%lib\
REM echo ******* LIB_HOME: LIB_HOME: %LIB_HOME%

SetLocal EnableDelayedExpansion

set classpath=%cmdppath%conf

for /F %%X in ('dir /B /D "%LIB_HOME%"') do (
  set tmp=%%X
  set classpath=!classpath!;%LIB_HOME%!tmp!
)
set LIB_HOME=%cmdppath%lib

REM echo ******** classpath: %classpath%

java -cp "%classpath%" -DCONTENTPUMP_HOME="%LIB_HOME%" -Dfile.encoding="UTF-8" %JVM_OPTS% com.marklogic.contentpump.ContentPump %*