@ECHO OFF
set argss=%*

set cmdpath=%~dp0
REM echo Command Path:  %cmdpath%
set cmdpath=%cmdpath:~0,-1%
for %%d in (%cmdpath%) do set cmdppath=%%~dpd
REM echo %cmdppath%
set LIB_HOME=%cmdppath%lib
REM echo LIB_HOME: %LIB_HOME%

SET "VMARGS=-DCONTENTPUMP_HOME=%LIB_HOME% -DCONTENTPUMP_VERSION=1.0"

SetLocal EnableDelayedExpansion

set classpath=%cmdppath%conf

for %%X in (%LIB_HOME%\*) do (
  set tmp=%%X
  set classpath=!classpath!;!tmp!
)
java -cp %classpath% %VMARGS% com.marklogic.contentpump.ContentPump %*
