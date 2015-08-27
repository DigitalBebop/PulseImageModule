logLevel := Level.Warn

resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.github.gseitz" % "sbt-protobuf" % "0.4.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")
