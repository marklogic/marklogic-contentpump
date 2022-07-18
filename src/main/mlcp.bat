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
SetLocal

set BUNDLE_ARTIFACT="apache"
if exist %cmdpath%\..\BUNDLE_ARTIFACT (
    set /p BUNDLE_ARTIFACT=<%cmdpath%\..\BUNDLE_ARTIFACT
)

set classpath=%cmdppath%conf
set HADOOP_HOME=%cmdppath%
set PATH=%PATH%;%HADOOP_HOME%bin

for /F %%X in ('dir /B /D "%LIB_HOME%"') do (
  set tmp=%%X
  set classpath=!classpath!;%LIB_HOME%!tmp!
)
set LIB_HOME=%cmdppath%lib\\

REM echo ******** classpath: %classpath%

java -DCONTENTPUMP_HOME="%LIB_HOME%" -DBUNDLE_ARTIFACT="%BUNDLE_ARTIFACT%" -Dfile.encoding="UTF-8" -Dxcc.txn.compatible=true %JVM_OPTS% com.marklogic.contentpump.ContentPump %*

EndLocal

exit /B %errorlevel%

