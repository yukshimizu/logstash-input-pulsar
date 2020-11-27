# this is a generated file, to avoid over-writing it just delete this comment
begin
  require 'jar_dependencies'
rescue LoadError
  require 'org/apache/commons/commons-lang3/3.4/commons-lang3-3.4.jar'
  require 'com/google/guava/guava/21.0/guava-21.0.jar'
  require 'com/github/luben/zstd-jni/1.3.7-3/zstd-jni-1.3.7-3.jar'
  require 'com/yahoo/athenz/athenz-zts-core/1.8.17/athenz-zts-core-1.8.17.jar'
  require 'org/bouncycastle/bcpkix-jdk15on/1.60/bcpkix-jdk15on-1.60.jar'
  require 'com/fasterxml/jackson/dataformat/jackson-dataformat-cbor/2.6.7/jackson-dataformat-cbor-2.6.7.jar'
  require 'com/yahoo/athenz/athenz-client-common/1.8.17/athenz-client-common-1.8.17.jar'
  require 'javax/xml/bind/jaxb-api/2.3.1/jaxb-api-2.3.1.jar'
  require 'org/bouncycastle/bcprov-jdk15on/1.60/bcprov-jdk15on-1.60.jar'
  require 'org/apache/httpcomponents/httpclient/4.5.5/httpclient-4.5.5.jar'
  require 'joda-time/joda-time/2.8.1/joda-time-2.8.1.jar'
  require 'org/apache/pulsar/pulsar-client-api/2.4.1/pulsar-client-api-2.4.1.jar'
  require 'com/fasterxml/jackson/core/jackson-databind/2.9.8/jackson-databind-2.9.8.jar'
  require 'org/apache/pulsar/protobuf-shaded/2.1.0-incubating/protobuf-shaded-2.1.0-incubating.jar'
  require 'com/yahoo/athenz/athenz-auth-core/1.8.17/athenz-auth-core-1.8.17.jar'
  require 'net/java/dev/jna/jna/4.5.2/jna-4.5.2.jar'
  require 'com/amazonaws/aws-java-sdk-core/1.11.502/aws-java-sdk-core-1.11.502.jar'
  require 'commons-codec/commons-codec/1.10/commons-codec-1.10.jar'
  require 'org/apache/pulsar/pulsar-client/2.4.1/pulsar-client-2.4.1.jar'
  require 'com/amazonaws/aws-java-sdk-sts/1.11.502/aws-java-sdk-sts-1.11.502.jar'
  require 'org/kohsuke/libpam4j/1.11/libpam4j-1.11.jar'
  require 'org/lz4/lz4-java/1.5.0/lz4-java-1.5.0.jar'
  require 'com/yahoo/rdl/rdl-java/1.5.2/rdl-java-1.5.2.jar'
  require 'com/sun/activation/javax.activation/1.2.0/javax.activation-1.2.0.jar'
  require 'org/apache/pulsar/pulsar-client-auth-athenz/2.4.1/pulsar-client-auth-athenz-2.4.1.jar'
  require 'javax/activation/javax.activation-api/1.2.0/javax.activation-api-1.2.0.jar'
  require 'org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar'
  require 'com/amazonaws/jmespath-java/1.11.502/jmespath-java-1.11.502.jar'
  require 'com/fasterxml/jackson/core/jackson-annotations/2.9.8/jackson-annotations-2.9.8.jar'
  require 'commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar'
  require 'software/amazon/ion/ion-java/1.0.2/ion-java-1.0.2.jar'
  require 'javax/validation/validation-api/1.1.0.Final/validation-api-1.1.0.Final.jar'
  require 'org/apache/httpcomponents/httpcore/4.4.9/httpcore-4.4.9.jar'
  require 'com/yahoo/athenz/athenz-zts-java-client/1.8.17/athenz-zts-java-client-1.8.17.jar'
  require 'com/yahoo/athenz/athenz-zms-core/1.8.17/athenz-zms-core-1.8.17.jar'
  require 'com/fasterxml/jackson/core/jackson-core/2.9.8/jackson-core-2.9.8.jar'
end

if defined? Jars
  require_jar 'org.apache.commons', 'commons-lang3', '3.4'
  require_jar 'com.google.guava', 'guava', '21.0'
  require_jar 'com.github.luben', 'zstd-jni', '1.3.7-3'
  require_jar 'com.yahoo.athenz', 'athenz-zts-core', '1.8.17'
  require_jar 'org.bouncycastle', 'bcpkix-jdk15on', '1.60'
  require_jar 'com.fasterxml.jackson.dataformat', 'jackson-dataformat-cbor', '2.6.7'
  require_jar 'com.yahoo.athenz', 'athenz-client-common', '1.8.17'
  require_jar 'javax.xml.bind', 'jaxb-api', '2.3.1'
  require_jar 'org.bouncycastle', 'bcprov-jdk15on', '1.60'
  require_jar 'org.apache.httpcomponents', 'httpclient', '4.5.5'
  require_jar 'joda-time', 'joda-time', '2.8.1'
  require_jar 'org.apache.pulsar', 'pulsar-client-api', '2.4.1'
  require_jar 'com.fasterxml.jackson.core', 'jackson-databind', '2.9.8'
  require_jar 'org.apache.pulsar', 'protobuf-shaded', '2.1.0-incubating'
  require_jar 'com.yahoo.athenz', 'athenz-auth-core', '1.8.17'
  require_jar 'net.java.dev.jna', 'jna', '4.5.2'
  require_jar 'com.amazonaws', 'aws-java-sdk-core', '1.11.502'
  require_jar 'commons-codec', 'commons-codec', '1.10'
  require_jar 'org.apache.pulsar', 'pulsar-client', '2.4.1'
  require_jar 'com.amazonaws', 'aws-java-sdk-sts', '1.11.502'
  require_jar 'org.kohsuke', 'libpam4j', '1.11'
  require_jar 'org.lz4', 'lz4-java', '1.5.0'
  require_jar 'com.yahoo.rdl', 'rdl-java', '1.5.2'
  require_jar 'com.sun.activation', 'javax.activation', '1.2.0'
  require_jar 'org.apache.pulsar', 'pulsar-client-auth-athenz', '2.4.1'
  require_jar 'javax.activation', 'javax.activation-api', '1.2.0'
  require_jar 'org.slf4j', 'slf4j-api', '1.7.25'
  require_jar 'com.amazonaws', 'jmespath-java', '1.11.502'
  require_jar 'com.fasterxml.jackson.core', 'jackson-annotations', '2.9.8'
  require_jar 'commons-logging', 'commons-logging', '1.1.3'
  require_jar 'software.amazon.ion', 'ion-java', '1.0.2'
  require_jar 'javax.validation', 'validation-api', '1.1.0.Final'
  require_jar 'org.apache.httpcomponents', 'httpcore', '4.4.9'
  require_jar 'com.yahoo.athenz', 'athenz-zts-java-client', '1.8.17'
  require_jar 'com.yahoo.athenz', 'athenz-zms-core', '1.8.17'
  require_jar 'com.fasterxml.jackson.core', 'jackson-core', '2.9.8'
end
