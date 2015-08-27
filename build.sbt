import sbtprotobuf.{ProtobufPlugin=>PB}
import AssemblyKeys._

name := "Pulse Files Module"

version := "1.0"

scalaVersion := "2.10.4"

Seq(PB.protobufSettings: _*)

version in protobufConfig := "2.6.1"

libraryDependencies ++= Seq(
  "commons-net" % "commons-net" % "3.3",
  "org.apache.httpcomponents" % "httpclient" % "4.5",
  "commons-io" % "commons-io" % "2.4",
  "org.apache.httpcomponents" % "httpclient" % "4.5",
  "commons-cli" % "commons-cli" % "1.3.1",
  "org.bouncycastle" % "bcprov-jdk15on" % "1.49",
  "org.bouncycastle" % "bcpkix-jdk15on" % "1.49",
  "org.apache.santuario" % "xmlsec" % "1.5.1",
  "com.itextpdf" % "itextpdf" % "5.5.6",
  "org.jsoup" % "jsoup" % "1.8.3",
  "com.unboundid" % "unboundid-ldapsdk" % "3.0.0"
)

assemblySettings

jarName in assembly := "imagesModule.jar"

mainClass in assembly := Some("net.digitalbebop.pulseimagemodule.Main")
