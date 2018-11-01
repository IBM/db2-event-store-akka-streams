lazy val akkaHttpVersion = "10.1.5"
lazy val akkaVersion    = "2.5.16"
lazy val sparkVersion = "2.3.2"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization    := "com.example",
      scalaVersion    := "2.11.12"
    )),
    name := "db2eventstoreakkastreams",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http"            % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-xml"        % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-stream"          % akkaVersion,

      "com.typesafe.akka" %% "akka-http-testkit"    % akkaHttpVersion % Test,
      "com.typesafe.akka" %% "akka-testkit"         % akkaVersion     % Test,
      "com.typesafe.akka" %% "akka-stream-testkit"  % akkaVersion     % Test,
      "com.novocode" % "junit-interface" % "0.11" % Test,
      "org.scalatest"     %% "scalatest"            % "3.0.1"         % Test
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark"    % "spark-core_2.11"   % sparkVersion,
      "org.apache.spark"    % "spark-sql_2.11"    % sparkVersion,
      "com.ibm.event" % "ibm-db2-eventstore-desktop-client" % "1.1.4" // For IBM Db2 Event Store Developer edition v1.1.4
    )
  )
