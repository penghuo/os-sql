import sbt.Keys.scalaVersion

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

lazy val scala212 = "2.12.14"
lazy val sparkVersion = "3.3.1"
lazy val opensearchVersion = "2.6.0"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := scala212

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.1.0")

lazy val root = (project in file("."))
  .aggregate(flintCore, flintSparkIntegration)
  .disablePlugins(AssemblyPlugin)
  .settings(
    name := "flint"
  )

lazy val flintCore = (project in file("flint-core"))
  .disablePlugins(AssemblyPlugin)
  .settings(
    name := "flint-core",
    scalaVersion := scala212
  )

lazy val flintSparkIntegration = (project in file("flint-spark-integration"))
  .dependsOn(flintCore)
  .enablePlugins(AssemblyPlugin)
  .settings(
    name := "flint-spark-integration",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.opensearch.client" % "opensearch-rest-client" % "2.6.0",
      "org.opensearch.client" % "opensearch-rest-high-level-client" % "2.6.0"
    ),
    scalaVersion := scala212,
    assemblyPackageScala / assembleArtifact := false,
    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
    },
    assembly / assemblyMergeStrategy := {
      case PathList(ps @ _*) if ps.last endsWith("module-info.class") => MergeStrategy.discard
      case PathList("module-info.class")                                 => MergeStrategy.discard
      case PathList("META-INF", "versions", xs @ _, "module-info.class") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

