run:
	JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
	PATH="$$JAVA_HOME/bin:$$PATH" \
	SBT_OPTS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" \
	sbt run
