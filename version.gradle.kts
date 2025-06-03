val stableVersion = "majanjua-SNAPSHOT"
val alphaVersion = "1.39.0-alpha"

allprojects {
  if (findProperty("otel.stable") != "true") {
    version = alphaVersion
  } else {
    version = stableVersion
  }
}
