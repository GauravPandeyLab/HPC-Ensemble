name := "DataSink_OpenSource"

version := "1.0"

scalaVersion := "2.11.8"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.2.0"

libraryDependencies += ("org.apache.spark" % "spark-mllib_2.11" % "2.2.0").
  exclude("org.mortbay.jetty", "servlet-api").
  exclude("commons-beanutils", "commons-beanutils-core").
  exclude("commons-collections", "commons-collections").
  exclude("com.esotericsoftware.minlog", "minlog").
  exclude("junit", "junit").
  exclude("org.slf4j", "log4j12").
  exclude("commons-logging", "commons-logging")

libraryDependencies += ("org.apache.spark" % "spark-mllib_2.11" % "2.2.0").
  exclude("org.mortbay.jetty", "servlet-api").
  exclude("commons-beanutils", "commons-beanutils-core").
  exclude("commons-collections", "commons-collections").
  exclude("com.esotericsoftware.minlog", "minlog").
  exclude("junit", "junit").
  exclude("org.slf4j", "log4j12").
  exclude("commons-logging", "commons-logging")

libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.3.0"

resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  "Typesafe repository mwn" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "https://repository.apache.org/content/repositories/orgapachespark-1153/" at "https://repository.apache.org/content/repositories/orgapachespark-1153/",
  Resolver.typesafeRepo("releases"),
  Resolver.sonatypeRepo("public"),
  Resolver.sbtPluginRepo("releases")
)
