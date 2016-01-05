name := "scala-spark-streaming-app"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.5.1",
	"org.apache.spark" %% "spark-mllib" % "1.5.1",
	"org.apache.spark" %% "spark-streaming" % "1.5.1",
	"org.apache.spark" %% "spark-graphx" % "1.5.1"
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases")
    