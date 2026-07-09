ThisBuild / organization := "io.github.iblislin"
ThisBuild / scalaVersion := "3.3.4" // LTS

lazy val root = (project in file("."))
  .settings(
    name    := "ui-serde-apicurio",
    version := "0.1.0-SNAPSHOT",
    libraryDependencies ++= Seq(
      // kafka-ui serde SPI. `provided` — kafka-ui supplies serde-api on the
      // plugin classloader at runtime; bundling it would risk a classloader clash.
      "io.kafbat.ui"    % "serde-api" % "1.0.0" % Provided,
      // Avro parses the .avsc AND renders the decoded record to JSON itself, so
      // the plugin needs NO separate JSON library (no Jackson-as-a-choice, no
      // jsoniter). Jackson only rides in transitively inside org.apache.avro.
      "org.apache.avro" % "avro"      % "1.12.0",
      "org.slf4j"       % "slf4j-api" % "2.0.16" % Provided,
      "org.scalameta"  %% "munit"     % "1.0.2"  % Test
    ),
    // Fat-jar: bundle the Scala stdlib + avro (+ its transitive jackson).
    // serde-api / slf4j are Provided → not bundled.
    assembly / assemblyJarName := s"${name.value}-${version.value}-assembly.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case "module-info.class"                  => MergeStrategy.discard
      case _                                    => MergeStrategy.first
    }
  )
